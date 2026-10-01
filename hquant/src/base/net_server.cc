#include "base/net_server.h"

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "base/error.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/redirect_error.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "boost/beast/core.hpp"
#include "boost/beast/websocket.hpp"

namespace hquant {
namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using Tcp = asio::ip::tcp;
}  // namespace

asio::awaitable<void> WebSocketServer::Serve(
    Tcp::socket socket, http::request<http::string_body> request,
    uint64_t session, MessageSource source) {
  websocket::stream<Tcp::socket> stream(std::move(socket));
  boost::system::error_code ec;
  co_await stream.async_accept(request,
                               asio::redirect_error(asio::use_awaitable, ec));
  if (ec) co_return;
  const std::string target(request.target());
  for (uint64_t ordinal = 0;; ++ordinal) {
    auto message = co_await source(session, ordinal, target);
    if (!message) break;
    stream.text(true);
    co_await stream.async_write(asio::buffer(*message),
                                asio::redirect_error(asio::use_awaitable, ec));
    if (ec) break;
  }
  boost::system::error_code ignored;
  stream.next_layer().close(ignored);
}

struct HttpServer::State : std::enable_shared_from_this<HttpServer::State> {
  asio::io_context& io;
  Tcp::acceptor acceptor;
  Handler handler;
  WebSocketServer::MessageSource websocket;
  std::atomic<bool> stopped{false};
  uint64_t next_session = 1;

  State(asio::io_context& context, Handler http_handler,
        WebSocketServer::MessageSource ws_source)
      : io(context),
        acceptor(context),
        handler(std::move(http_handler)),
        websocket(std::move(ws_source)) {}

  asio::awaitable<void> Session(Tcp::socket socket, uint64_t id) {
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    boost::system::error_code ec;
    co_await http::async_read(socket, buffer, request,
                              asio::redirect_error(asio::use_awaitable, ec));
    if (ec) co_return;
    if (websocket::is_upgrade(request)) {
      co_await WebSocketServer::Serve(std::move(socket), std::move(request), id,
                                      websocket);
      co_return;
    }
    auto value = handler(std::string(request.method_string()),
                         std::string(request.target()));
    http::response<http::string_body> reply{
        static_cast<http::status>(value.status), request.version()};
    reply.set(http::field::server, "hquant-bench-feed");
    reply.keep_alive(false);
    for (const auto& [key, item] : value.headers) reply.set(key, item);
    reply.body() = std::move(value.body);
    reply.prepare_payload();
    co_await http::async_write(socket, reply,
                               asio::redirect_error(asio::use_awaitable, ec));
    socket.close(ec);
  }

  asio::awaitable<void> Accept() {
    while (!stopped) {
      boost::system::error_code ec;
      Tcp::socket socket = co_await acceptor.async_accept(
          asio::redirect_error(asio::use_awaitable, ec));
      if (ec) {
        if (stopped) break;
        continue;
      }
      asio::co_spawn(io, Session(std::move(socket), next_session++),
                     asio::detached);
    }
  }
};

HttpServer::HttpServer(std::shared_ptr<State> state)
    : state_(std::move(state)) {}
HttpServer::~HttpServer() { Stop(); }

absl::StatusOr<std::unique_ptr<HttpServer>> HttpServer::Start(
    asio::io_context& io, std::string address, uint16_t port, Handler handler,
    WebSocketServer::MessageSource websocket) {
  boost::system::error_code ec;
  auto ip = asio::ip::make_address(address, ec);
  if (ec) return Error(ErrorCode::kNetTargetInvalid, "invalid listen address");
  auto state =
      std::make_shared<State>(io, std::move(handler), std::move(websocket));
  state->acceptor.open(ip.is_v4() ? Tcp::v4() : Tcp::v6(), ec);
  if (!ec) state->acceptor.set_option(Tcp::acceptor::reuse_address(true), ec);
  if (!ec) state->acceptor.bind(Tcp::endpoint(ip, port), ec);
  if (!ec)
    state->acceptor.listen(asio::socket_base::max_listen_connections, ec);
  if (ec) return Error(ErrorCode::kNetUnavailable, ec.message());
  asio::co_spawn(io, state->Accept(), asio::detached);
  return std::unique_ptr<HttpServer>(new HttpServer(std::move(state)));
}

void HttpServer::Stop() {
  if (!state_ || state_->stopped.exchange(true)) return;
  boost::system::error_code ignored;
  state_->acceptor.close(ignored);
}

uint16_t HttpServer::bound_port() const {
  boost::system::error_code ec;
  const auto local = state_->acceptor.local_endpoint(ec);
  return ec ? 0 : local.port();
}

}  // namespace hquant
