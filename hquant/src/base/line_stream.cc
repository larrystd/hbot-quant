#include "base/line_stream.h"

#include <sys/un.h>

#include <atomic>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <utility>

#include "base/error.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/post.hpp"
#include "boost/asio/redirect_error.hpp"
#include "boost/asio/use_awaitable.hpp"

namespace hquant {
namespace {
namespace asio = boost::asio;
using Deadline = std::chrono::steady_clock::time_point;

absl::Status IoStatus(const boost::system::error_code& ec, bool expired) {
  if (expired || ec == asio::error::timed_out)
    return Error(ErrorCode::kNetTimeout, "line operation timed out");
  if (ec == asio::error::eof || ec == asio::error::connection_reset ||
      ec == asio::error::broken_pipe)
    return Error(ErrorCode::kNetNotConnected, ec.message());
  return Error(ErrorCode::kNetUnavailable, ec.message());
}

template <typename Socket>
asio::awaitable<absl::StatusOr<std::string>> ReadFrame(Socket& socket,
                                                        size_t max_frame,
                                                        Deadline deadline) {
  if (std::chrono::steady_clock::now() >= deadline)
    co_return Error(ErrorCode::kNetTimeout, "line read deadline elapsed");
  asio::streambuf buffer(max_frame + 1);
  asio::steady_timer timer(socket.get_executor());
  auto expired = std::make_shared<std::atomic<bool>>(false);
  timer.expires_at(deadline);
  timer.async_wait([&socket, expired](const boost::system::error_code& ec) {
    if (!ec) {
      *expired = true;
      boost::system::error_code ignored;
      socket.cancel(ignored);
    }
  });
  boost::system::error_code ec;
  const size_t size = co_await asio::async_read_until(
      socket, buffer, '\n', asio::redirect_error(asio::use_awaitable, ec));
  timer.cancel();
  if (ec == asio::error::not_found)
    co_return Error(ErrorCode::kNetTargetInvalid, "line frame too large");
  if (ec) co_return IoStatus(ec, *expired);
  if (size == 0 || size - 1 > max_frame)
    co_return Error(ErrorCode::kNetTargetInvalid, "line frame too large");
  std::string result(size - 1, '\0');
  asio::buffer_copy(asio::buffer(result), buffer.data(), size - 1);
  co_return result;
}

template <typename Socket>
asio::awaitable<absl::Status> WriteFrame(Socket& socket, std::string line,
                                        Deadline deadline) {
  if (std::chrono::steady_clock::now() >= deadline)
    co_return Error(ErrorCode::kNetTimeout, "line write deadline elapsed");
  line.push_back('\n');
  asio::steady_timer timer(socket.get_executor());
  auto expired = std::make_shared<std::atomic<bool>>(false);
  timer.expires_at(deadline);
  timer.async_wait([&socket, expired](const boost::system::error_code& ec) {
    if (!ec) {
      *expired = true;
      boost::system::error_code ignored;
      socket.cancel(ignored);
    }
  });
  boost::system::error_code ec;
  co_await asio::async_write(
      socket, asio::buffer(line), asio::redirect_error(asio::use_awaitable, ec));
  timer.cancel();
  if (ec) co_return IoStatus(ec, *expired);
  co_return absl::OkStatus();
}

template <typename Socket, typename Endpoint>
asio::awaitable<absl::Status> ConnectSocket(Socket& socket,
                                            const Endpoint& endpoint,
                                            Deadline deadline) {
  if (std::chrono::steady_clock::now() >= deadline)
    co_return Error(ErrorCode::kNetTimeout, "line connect deadline elapsed");
  asio::steady_timer timer(socket.get_executor());
  auto expired = std::make_shared<std::atomic<bool>>(false);
  timer.expires_at(deadline);
  timer.async_wait([&socket, expired](const boost::system::error_code& ec) {
    if (!ec) {
      *expired = true;
      boost::system::error_code ignored;
      socket.cancel(ignored);
    }
  });
  boost::system::error_code ec;
  co_await socket.async_connect(
      endpoint, asio::redirect_error(asio::use_awaitable, ec));
  timer.cancel();
  if (ec) co_return IoStatus(ec, *expired);
  co_return absl::OkStatus();
}

template <typename Socket>
void CloseSocket(Socket& socket) {
  boost::system::error_code ignored;
  socket.close(ignored);
}

}  // namespace

LineClient::LineClient(asio::io_context& io, LineEndpoint endpoint,
                       size_t max_frame)
    : io_(io), endpoint_(std::move(endpoint)), max_frame_(max_frame) {}

asio::awaitable<absl::Status> LineClient::Connect(Deadline deadline) {
  if (in_flight_)
    co_return Error(ErrorCode::kNetConcurrentCall, "concurrent line operation");
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  Close();
  if (endpoint_.kind == LineEndpoint::Kind::Unix) {
    if (endpoint_.address.empty() ||
        endpoint_.address.size() >= sizeof(sockaddr_un::sun_path))
      co_return Error(ErrorCode::kNetTargetInvalid, "invalid Unix socket path");
    unix_ = std::make_unique<asio::local::stream_protocol::socket>(io_);
    co_return co_await ConnectSocket(
        *unix_, asio::local::stream_protocol::endpoint(endpoint_.address),
        deadline);
  }
  boost::system::error_code ec;
  auto address = asio::ip::make_address(endpoint_.address, ec);
  if (ec || endpoint_.port == 0)
    co_return Error(ErrorCode::kNetTargetInvalid, "invalid TCP endpoint");
  tcp_ = std::make_unique<asio::ip::tcp::socket>(io_);
  co_return co_await ConnectSocket(
      *tcp_, asio::ip::tcp::endpoint(address, endpoint_.port), deadline);
}

asio::awaitable<absl::Status> LineClient::Send(std::string line,
                                               Deadline deadline) {
  if (in_flight_)
    co_return Error(ErrorCode::kNetConcurrentCall, "concurrent line operation");
  if (line.size() > max_frame_ || line.find('\n') != std::string::npos)
    co_return Error(ErrorCode::kNetTargetInvalid, "invalid line frame");
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  if (unix_ && unix_->is_open())
    co_return co_await WriteFrame(*unix_, std::move(line), deadline);
  if (tcp_ && tcp_->is_open())
    co_return co_await WriteFrame(*tcp_, std::move(line), deadline);
  co_return Error(ErrorCode::kNetNotConnected, "line client not connected");
}

asio::awaitable<absl::StatusOr<std::string>> LineClient::Receive(
    Deadline deadline) {
  if (in_flight_)
    co_return Error(ErrorCode::kNetConcurrentCall, "concurrent line operation");
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  if (unix_ && unix_->is_open())
    co_return co_await ReadFrame(*unix_, max_frame_, deadline);
  if (tcp_ && tcp_->is_open())
    co_return co_await ReadFrame(*tcp_, max_frame_, deadline);
  co_return Error(ErrorCode::kNetNotConnected, "line client not connected");
}

void LineClient::Close() {
  if (unix_) CloseSocket(*unix_);
  if (tcp_) CloseSocket(*tcp_);
  unix_.reset();
  tcp_.reset();
}

struct LineServer::State : std::enable_shared_from_this<LineServer::State> {
  asio::io_context& io;
  LineEndpoint endpoint;
  Handler handler;
  Options options;
  std::unique_ptr<asio::local::stream_protocol::acceptor> unix_acceptor;
  std::unique_ptr<asio::ip::tcp::acceptor> tcp_acceptor;
  std::unordered_map<uint64_t, std::function<void()>> sessions;
  std::atomic<bool> stopping{false};
  uint64_t next_id = 1;
  uint16_t bound_port = 0;

  State(asio::io_context& io_arg, LineEndpoint endpoint_arg,
        Handler handler_arg, Options options_arg)
      : io(io_arg), endpoint(std::move(endpoint_arg)),
        handler(std::move(handler_arg)), options(std::move(options_arg)) {}

  template <typename Socket>
  asio::awaitable<void> Reply(std::shared_ptr<Socket> socket,
                               std::string line) {
    if (!line.empty())
      (void)co_await WriteFrame(*socket, std::move(line),
                                std::chrono::steady_clock::now() +
                                    options.write_timeout);
    CloseSocket(*socket);
  }

  template <typename Socket>
  asio::awaitable<void> Session(std::shared_ptr<Socket> socket) {
    auto frame = co_await ReadFrame(*socket, options.max_frame,
                                    std::chrono::steady_clock::now() +
                                        options.read_timeout);
    if (!frame.ok()) {
      if (CodeOf(frame.status()) == ErrorCode::kNetTargetInvalid)
        co_await Reply(socket, options.invalid_reply);
      else CloseSocket(*socket);
      co_return;
    }
    try {
      auto response = co_await handler(std::move(*frame));
      co_await Reply(socket, std::move(response));
    } catch (...) {
      CloseSocket(*socket);
    }
  }

  template <typename Socket>
  void Dispatch(std::shared_ptr<Socket> socket) {
    if (sessions.size() >= options.max_sessions) {
      asio::co_spawn(io, Reply(socket, options.busy_reply), asio::detached);
      return;
    }
    const uint64_t id = next_id++;
    sessions.emplace(id, [socket] { CloseSocket(*socket); });
    auto self = shared_from_this();
    asio::co_spawn(io, Session(socket),
                   [self, id](std::exception_ptr) { self->sessions.erase(id); });
  }

  asio::awaitable<void> AcceptUnix() {
    while (!stopping) {
      auto socket =
          std::make_shared<asio::local::stream_protocol::socket>(io);
      boost::system::error_code ec;
      co_await unix_acceptor->async_accept(
          *socket, asio::redirect_error(asio::use_awaitable, ec));
      if (ec) break;
      Dispatch(std::move(socket));
    }
  }

  asio::awaitable<void> AcceptTcp() {
    while (!stopping) {
      auto socket = std::make_shared<asio::ip::tcp::socket>(io);
      boost::system::error_code ec;
      co_await tcp_acceptor->async_accept(
          *socket, asio::redirect_error(asio::use_awaitable, ec));
      if (ec) break;
      Dispatch(std::move(socket));
    }
  }

  void CloseAcceptors() {
    if (unix_acceptor) {
      boost::system::error_code ignored;
      unix_acceptor->close(ignored);
    }
    if (tcp_acceptor) {
      boost::system::error_code ignored;
      tcp_acceptor->close(ignored);
    }
  }
};

LineServer::LineServer(std::shared_ptr<State> state)
    : state_(std::move(state)) {}

absl::StatusOr<std::unique_ptr<LineServer>> LineServer::Start(
    asio::io_context& io, LineEndpoint endpoint, Handler handler,
    Options options) {
  if (!handler || options.max_sessions == 0 || options.max_frame == 0 ||
      options.read_timeout <= std::chrono::steady_clock::duration::zero() ||
      options.write_timeout <= std::chrono::steady_clock::duration::zero())
    return Error(ErrorCode::kNetTargetInvalid, "invalid line server options");
  auto state =
      std::make_shared<State>(io, std::move(endpoint), std::move(handler),
                              std::move(options));
  boost::system::error_code ec;
  if (state->endpoint.kind == LineEndpoint::Kind::Unix) {
    const auto& path = state->endpoint.address;
    if (path.empty() || path.size() >= sizeof(sockaddr_un::sun_path))
      return Error(ErrorCode::kControlSocketFailed, "invalid Unix socket path");
    if (std::filesystem::exists(path)) {
      asio::local::stream_protocol::socket probe(io);
      probe.connect(asio::local::stream_protocol::endpoint(path), ec);
      if (!ec)
        return Error(ErrorCode::kControlSocketInUse, "server socket is in use");
      std::filesystem::remove(path, ec);
      if (ec) return Error(ErrorCode::kControlSocketFailed, ec.message());
    }
    state->unix_acceptor =
        std::make_unique<asio::local::stream_protocol::acceptor>(io);
    state->unix_acceptor->open(asio::local::stream_protocol(), ec);
    if (!ec)
      state->unix_acceptor->bind(
          asio::local::stream_protocol::endpoint(path), ec);
    if (!ec) state->unix_acceptor->listen(asio::socket_base::max_listen_connections, ec);
    if (ec) return Error(ErrorCode::kControlSocketFailed, ec.message());
    asio::co_spawn(io, state->AcceptUnix(), asio::detached);
  } else {
    auto address = asio::ip::make_address(state->endpoint.address, ec);
    if (ec) return Error(ErrorCode::kNetTargetInvalid, "invalid TCP address");
    state->tcp_acceptor = std::make_unique<asio::ip::tcp::acceptor>(io);
    state->tcp_acceptor->open(asio::ip::tcp::v4(), ec);
    if (!ec) state->tcp_acceptor->set_option(asio::socket_base::reuse_address(true), ec);
    if (!ec)
      state->tcp_acceptor->bind(
          asio::ip::tcp::endpoint(address, state->endpoint.port), ec);
    if (!ec) state->tcp_acceptor->listen(asio::socket_base::max_listen_connections, ec);
    if (ec) return Error(ErrorCode::kNetUnavailable, ec.message());
    state->bound_port = state->tcp_acceptor->local_endpoint().port();
    asio::co_spawn(io, state->AcceptTcp(), asio::detached);
  }
  return std::unique_ptr<LineServer>(new LineServer(std::move(state)));
}

LineServer::~LineServer() { Stop(); }

void LineServer::Stop(bool remove_unix_path) {
  if (!state_) return;
  if (!state_->stopping.exchange(true)) {
    auto state = state_;
    asio::post(state->io, [state] { state->CloseAcceptors(); });
  }
  if (remove_unix_path && state_->endpoint.kind == LineEndpoint::Kind::Unix) {
    std::error_code ignored;
    std::filesystem::remove(state_->endpoint.address, ignored);
  }
}

uint16_t LineServer::bound_port() const { return state_->bound_port; }

}  // namespace hquant
