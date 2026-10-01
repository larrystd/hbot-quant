#include "hbot/net/websocket_client.h"

#include <utility>

#include "boost/asio/redirect_error.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "boost/beast/websocket/ssl.hpp"
#include "openssl/ssl.h"

namespace hbot {
namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
}  // namespace

WebSocketClient::WebSocketClient(asio::io_context& io, std::string host,
                                 std::string port, std::string target,
                                 TlsConfig tls)
    : io_(io),
      host_(std::move(host)),
      port_(std::move(port)),
      target_(std::move(target)),
      tls_(std::move(tls)),
      ssl_context_(asio::ssl::context::tls_client),
      tls_status_(ConfigureTlsContext(ssl_context_, tls_)),
      resolver_(io),
      resolver_deadline_(io),
      operation_deadline_(io) {}

void WebSocketClient::ArmDeadline(
    std::chrono::steady_clock::time_point deadline) {
  operation_deadline_.expires_at(deadline);
  operation_deadline_.async_wait([this](const boost::system::error_code& ec) {
    if (ec) return;
    boost::system::error_code ignored;
    if (plain_) beast::get_lowest_layer(*plain_).socket().close(ignored);
    if (secure_) beast::get_lowest_layer(*secure_).socket().close(ignored);
  });
}

void WebSocketClient::DisarmDeadline() { operation_deadline_.cancel(); }

absl::Status WebSocketClient::Error(
    const boost::system::error_code& ec,
    std::chrono::steady_clock::time_point deadline) const {
  if (std::chrono::steady_clock::now() >= deadline ||
      ec == beast::error::timeout || ec == asio::error::timed_out) {
    return absl::DeadlineExceededError(ec.message());
  }
  if (cancelled_ || ec == asio::error::operation_aborted) {
    return absl::CancelledError(ec.message());
  }
  return absl::UnavailableError(ec.message());
}

void WebSocketClient::DropConnection() {
  DisarmDeadline();
  boost::system::error_code ignored;
  if (plain_) beast::get_lowest_layer(*plain_).socket().close(ignored);
  if (secure_) beast::get_lowest_layer(*secure_).socket().close(ignored);
  plain_.reset();
  secure_.reset();
  connected_ = false;
}

void WebSocketClient::Cancel() {
  cancelled_ = true;
  resolver_.cancel();
  DisarmDeadline();
  connected_ = false;
  if (in_flight_) {
    boost::system::error_code ignored;
    if (plain_) beast::get_lowest_layer(*plain_).socket().close(ignored);
    if (secure_) beast::get_lowest_layer(*secure_).socket().close(ignored);
  } else {
    DropConnection();
  }
}

bool WebSocketClient::Connected() const { return connected_; }

asio::awaitable<absl::Status> WebSocketClient::Connect(
    std::chrono::steady_clock::time_point deadline) {
  if (in_flight_)
    co_return absl::FailedPreconditionError("concurrent WebSocket operation");
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  cancelled_ = false;
  if (!tls_status_.ok()) co_return tls_status_;
  if (target_.empty() || target_[0] != '/') {
    co_return absl::InvalidArgumentError("WebSocket target invalid");
  }
  if (connected_) co_return absl::OkStatus();
  DropConnection();
  if (std::chrono::steady_clock::now() >= deadline) {
    co_return absl::DeadlineExceededError("WebSocket connect deadline elapsed");
  }
  boost::system::error_code ec;
  resolver_deadline_.expires_at(deadline);
  resolver_deadline_.async_wait(
      [this](const boost::system::error_code& timer_ec) {
        if (!timer_ec) resolver_.cancel();
      });
  auto endpoints = co_await resolver_.async_resolve(
      host_, port_, asio::redirect_error(asio::use_awaitable, ec));
  resolver_deadline_.cancel();
  if (ec) co_return Error(ec, deadline);

  const std::string host_header = host_ + ":" + port_;
  if (!tls_.enabled) {
    plain_ = std::make_unique<PlainStream>(io_);
    ArmDeadline(deadline);
    beast::get_lowest_layer(*plain_).expires_at(deadline);
    co_await beast::get_lowest_layer(*plain_).async_connect(
        endpoints, asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      DropConnection();
      co_return Error(ec, deadline);
    }
    beast::get_lowest_layer(*plain_).expires_at(deadline);
    co_await plain_->async_handshake(
        host_header, target_, asio::redirect_error(asio::use_awaitable, ec));
  } else {
    secure_ = std::make_unique<SecureStream>(io_, ssl_context_);
    const std::string& name =
        tls_.server_name.empty() ? host_ : tls_.server_name;
    if (!SSL_set_tlsext_host_name(secure_->next_layer().native_handle(),
                                  name.c_str())) {
      DropConnection();
      co_return absl::InvalidArgumentError("TLS SNI setup failed");
    }
    if (tls_.verify_peer) {
      secure_->next_layer().set_verify_callback(
          asio::ssl::host_name_verification(name));
    }
    ArmDeadline(deadline);
    beast::get_lowest_layer(*secure_).expires_at(deadline);
    co_await beast::get_lowest_layer(*secure_).async_connect(
        endpoints, asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      DropConnection();
      co_return Error(ec, deadline);
    }
    co_await secure_->next_layer().async_handshake(
        asio::ssl::stream_base::client,
        asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      DropConnection();
      co_return Error(ec, deadline);
    }
    beast::get_lowest_layer(*secure_).expires_at(deadline);
    co_await secure_->async_handshake(
        host_header, target_, asio::redirect_error(asio::use_awaitable, ec));
  }
  DisarmDeadline();
  if (ec) {
    DropConnection();
    co_return Error(ec, deadline);
  }
  connected_ = true;
  co_return absl::OkStatus();
}

asio::awaitable<absl::Status> WebSocketClient::Reconnect(
    std::chrono::steady_clock::time_point deadline) {
  if (in_flight_)
    co_return absl::FailedPreconditionError("concurrent WebSocket operation");
  DropConnection();
  co_return co_await Connect(deadline);
}

asio::awaitable<absl::Status> WebSocketClient::WriteText(
    std::string message, std::chrono::steady_clock::time_point deadline) {
  if (!connected_ || in_flight_) {
    co_return absl::FailedPreconditionError("WebSocket unavailable or busy");
  }
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  cancelled_ = false;
  boost::system::error_code ec;
  ArmDeadline(deadline);
  if (plain_) {
    plain_->text(true);
    beast::get_lowest_layer(*plain_).expires_at(deadline);
    co_await plain_->async_write(asio::buffer(message),
                                 asio::redirect_error(asio::use_awaitable, ec));
  } else {
    secure_->text(true);
    beast::get_lowest_layer(*secure_).expires_at(deadline);
    co_await secure_->async_write(
        asio::buffer(message), asio::redirect_error(asio::use_awaitable, ec));
  }
  DisarmDeadline();
  if (ec) {
    DropConnection();
    co_return Error(ec, deadline);
  }
  co_return absl::OkStatus();
}

asio::awaitable<absl::StatusOr<std::string>> WebSocketClient::Read(
    std::chrono::steady_clock::time_point deadline) {
  if (!connected_ || in_flight_) {
    co_return absl::FailedPreconditionError("WebSocket unavailable or busy");
  }
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  cancelled_ = false;
  beast::flat_buffer buffer;
  boost::system::error_code ec;
  ArmDeadline(deadline);
  if (plain_) {
    beast::get_lowest_layer(*plain_).expires_at(deadline);
    co_await plain_->async_read(buffer,
                                asio::redirect_error(asio::use_awaitable, ec));
  } else {
    beast::get_lowest_layer(*secure_).expires_at(deadline);
    co_await secure_->async_read(buffer,
                                 asio::redirect_error(asio::use_awaitable, ec));
  }
  DisarmDeadline();
  if (ec) {
    DropConnection();
    co_return Error(ec, deadline);
  }
  co_return beast::buffers_to_string(buffer.data());
}

asio::awaitable<absl::Status> WebSocketClient::Close(
    std::chrono::steady_clock::time_point deadline) {
  if (!connected_) {
    DropConnection();
    co_return absl::OkStatus();
  }
  if (in_flight_) co_return absl::FailedPreconditionError("WebSocket busy");
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  boost::system::error_code ec;
  ArmDeadline(deadline);
  if (plain_) {
    beast::get_lowest_layer(*plain_).expires_at(deadline);
    co_await plain_->async_close(websocket::close_code::normal,
                                 asio::redirect_error(asio::use_awaitable, ec));
  } else {
    beast::get_lowest_layer(*secure_).expires_at(deadline);
    co_await secure_->async_close(
        websocket::close_code::normal,
        asio::redirect_error(asio::use_awaitable, ec));
  }
  DisarmDeadline();
  DropConnection();
  if (ec) co_return Error(ec, deadline);
  co_return absl::OkStatus();
}

}  // namespace hbot
