#include "base/net.h"

#include <charconv>
#include <cstdint>

#include "absl/status/status.h"
#include "base/error.h"
#include "boost/asio/ssl.hpp"
#include "boost/system/error_code.hpp"

namespace hquant {

absl::Status ConfigureTlsContext(boost::asio::ssl::context& context,
                                 const TlsConfig& config) {
  if (!config.enabled) return absl::OkStatus();
  boost::system::error_code ec;
  context.set_options(boost::asio::ssl::context::default_workarounds, ec);
  if (ec) return hquant::Error(ErrorCode::kNetTlsConfigInvalid, ec.message());
  if (config.verify_peer) {
    if (config.ca_file.empty())
      context.set_default_verify_paths(ec);
    else
      context.load_verify_file(config.ca_file, ec);
    if (ec) return hquant::Error(ErrorCode::kNetTlsConfigInvalid, ec.message());
    context.set_verify_mode(boost::asio::ssl::verify_peer);
  } else {
    context.set_verify_mode(boost::asio::ssl::verify_none);
  }
  return absl::OkStatus();
}

}  // namespace hquant

#include <utility>

#include "boost/asio/redirect_error.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "boost/beast/http.hpp"
#include "openssl/ssl.h"

namespace hquant {
namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;

absl::Status NetworkError(const boost::system::error_code& ec,
                          std::chrono::steady_clock::time_point deadline,
                          bool cancelled) {
  if (std::chrono::steady_clock::now() >= deadline ||
      ec == beast::error::timeout || ec == asio::error::timed_out) {
    return hquant::Error(ErrorCode::kNetTimeout, ec.message());
  }
  if (cancelled || ec == asio::error::operation_aborted) {
    return hquant::Error(ErrorCode::kCancelled, ec.message());
  }
  return hquant::Error(ErrorCode::kNetUnavailable, ec.message());
}

bool StaleConnectionError(const boost::system::error_code& ec) {
  return ec == asio::error::eof || ec == asio::error::connection_reset ||
         ec == asio::error::broken_pipe || ec == asio::error::not_connected ||
         ec == http::error::end_of_stream;
}

}  // namespace

HttpClient::HttpClient(asio::io_context& io, std::string host, std::string port,
                       TlsConfig tls)
    : io_(io),
      host_(std::move(host)),
      port_(std::move(port)),
      tls_(std::move(tls)),
      ssl_context_(asio::ssl::context::tls_client),
      tls_status_(ConfigureTlsContext(ssl_context_, tls_)),
      resolver_(io),
      resolver_deadline_(io) {}

void HttpClient::DropConnection() {
  boost::system::error_code ignored;
  if (plain_) beast::get_lowest_layer(*plain_).socket().close(ignored);
  if (secure_) beast::get_lowest_layer(*secure_).socket().close(ignored);
  plain_.reset();
  secure_.reset();
}

void HttpClient::Cancel() {
  cancelled_ = true;
  resolver_.cancel();
  // Closing the socket completes an outstanding async operation. Its stream
  // object must remain alive until the awaiting coroutine resumes.
  if (in_flight_) {
    boost::system::error_code ignored;
    if (plain_) beast::get_lowest_layer(*plain_).socket().close(ignored);
    if (secure_) beast::get_lowest_layer(*secure_).socket().close(ignored);
  } else {
    DropConnection();
  }
}

bool HttpClient::Connected() const {
  return (plain_ && beast::get_lowest_layer(*plain_).socket().is_open()) ||
         (secure_ && beast::get_lowest_layer(*secure_).socket().is_open());
}

asio::awaitable<absl::Status> HttpClient::Connect(
    std::chrono::steady_clock::time_point deadline) {
  if (Connected()) co_return absl::OkStatus();
  if (std::chrono::steady_clock::now() >= deadline) {
    co_return hquant::Error(ErrorCode::kNetTimeout,
                            "HTTP connect deadline elapsed");
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
  if (ec) co_return NetworkError(ec, deadline, cancelled_);

  if (!tls_.enabled) {
    plain_ = std::make_unique<PlainStream>(io_);
    plain_->expires_at(deadline);
    co_await plain_->async_connect(
        endpoints, asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      DropConnection();
      co_return NetworkError(ec, deadline, cancelled_);
    }
  } else {
    secure_ = std::make_unique<SecureStream>(io_, ssl_context_);
    const std::string& name =
        tls_.server_name.empty() ? host_ : tls_.server_name;
    if (!SSL_set_tlsext_host_name(secure_->native_handle(), name.c_str())) {
      DropConnection();
      co_return hquant::Error(ErrorCode::kNetTlsConfigInvalid,
                              "TLS SNI setup failed");
    }
    if (tls_.verify_peer) {
      secure_->set_verify_callback(asio::ssl::host_name_verification(name));
    }
    beast::get_lowest_layer(*secure_).expires_at(deadline);
    co_await beast::get_lowest_layer(*secure_).async_connect(
        endpoints, asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      DropConnection();
      co_return NetworkError(ec, deadline, cancelled_);
    }
    co_await secure_->async_handshake(
        asio::ssl::stream_base::client,
        asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      const bool verify_failed =
          tls_.verify_peer &&
          SSL_get_verify_result(secure_->native_handle()) != X509_V_OK;
      DropConnection();
      if (verify_failed)
        co_return hquant::Error(ErrorCode::kNetTlsVerifyFailed, ec.message());
      co_return NetworkError(ec, deadline, cancelled_);
    }
  }
  co_return absl::OkStatus();
}

asio::awaitable<absl::StatusOr<HttpResponse>> HttpClient::Send(
    HttpRequest request) {
  if (in_flight_)
    co_return hquant::Error(ErrorCode::kNetBusy, "concurrent HTTP Send");
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  cancelled_ = false;
  if (!tls_status_.ok()) co_return tls_status_;
  if (request.method.empty() || request.target.empty() ||
      request.target[0] != '/') {
    co_return hquant::Error(ErrorCode::kNetTargetInvalid,
                            "HTTP method or target invalid");
  }
  if (std::chrono::steady_clock::now() >= request.deadline) {
    co_return hquant::Error(ErrorCode::kNetTimeout,
                            "HTTP request deadline elapsed");
  }
  http::request<http::string_body> wire;
  wire.version(11);
  wire.method_string(request.method);
  wire.target(request.target);
  wire.set(http::field::host, host_);
  wire.set(http::field::user_agent, "hquant-cpp/0.1");
  wire.keep_alive(true);
  for (const auto& [key, value] : request.headers) wire.set(key, value);
  wire.body() = request.body;
  wire.prepare_payload();

  for (int attempt = 0; attempt < 2; ++attempt) {
    const bool reused = Connected();
    auto status = co_await Connect(request.deadline);
    if (!status.ok()) co_return status;

    boost::system::error_code ec;
    if (plain_) {
      plain_->expires_at(request.deadline);
      co_await http::async_write(*plain_, wire,
                                 asio::redirect_error(asio::use_awaitable, ec));
    } else {
      beast::get_lowest_layer(*secure_).expires_at(request.deadline);
      co_await http::async_write(*secure_, wire,
                                 asio::redirect_error(asio::use_awaitable, ec));
    }
    if (!ec) {
      beast::flat_buffer buffer;
      http::response_parser<http::string_body> parser;
      parser.body_limit(8 * 1024 * 1024);
      if (plain_) {
        plain_->expires_at(request.deadline);
        co_await http::async_read(
            *plain_, buffer, parser,
            asio::redirect_error(asio::use_awaitable, ec));
      } else {
        beast::get_lowest_layer(*secure_).expires_at(request.deadline);
        co_await http::async_read(
            *secure_, buffer, parser,
            asio::redirect_error(asio::use_awaitable, ec));
      }
      if (!ec) {
        auto response = parser.release();
        HttpResponse result{response.result_int(), std::move(response.body())};
        const auto retry_after = response[http::field::retry_after];
        if (!retry_after.empty()) {
          uint64_t seconds = 0;
          const auto [end, parse_error] =
              std::from_chars(retry_after.data(),
                              retry_after.data() + retry_after.size(), seconds);
          if (parse_error == std::errc{} &&
              end == retry_after.data() + retry_after.size() &&
              seconds <= static_cast<uint64_t>(
                             std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::steady_clock::duration::max())
                                 .count())) {
            result.retry_after = std::chrono::seconds(seconds);
          }
        }
        if (!response.keep_alive()) DropConnection();
        co_return result;
      }
    }
    const bool retry = attempt == 0 && reused && request.method == "GET" &&
                       !cancelled_ &&
                       std::chrono::steady_clock::now() < request.deadline &&
                       StaleConnectionError(ec);
    const auto error = NetworkError(ec, request.deadline, cancelled_);
    DropConnection();
    if (!retry) co_return error;
  }
  co_return hquant::Error(ErrorCode::kNetUnavailable,
                          "stale HTTP connection retry exhausted");
}

}  // namespace hquant

#include "boost/beast/websocket/ssl.hpp"

namespace hquant {
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
    return hquant::Error(ErrorCode::kNetTimeout, ec.message());
  }
  if (cancelled_ || ec == asio::error::operation_aborted) {
    return hquant::Error(ErrorCode::kCancelled, ec.message());
  }
  return hquant::Error(ErrorCode::kNetUnavailable, ec.message());
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
    co_return hquant::Error(ErrorCode::kNetBusy,
                            "concurrent WebSocket operation");
  in_flight_ = true;
  struct Reset {
    bool& value;
    ~Reset() { value = false; }
  } reset{in_flight_};
  cancelled_ = false;
  if (!tls_status_.ok()) co_return tls_status_;
  if (target_.empty() || target_[0] != '/') {
    co_return hquant::Error(ErrorCode::kNetTargetInvalid,
                            "WebSocket target invalid");
  }
  if (connected_) co_return absl::OkStatus();
  DropConnection();
  if (std::chrono::steady_clock::now() >= deadline) {
    co_return hquant::Error(ErrorCode::kNetTimeout,
                            "WebSocket connect deadline elapsed");
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
      co_return hquant::Error(ErrorCode::kNetTlsConfigInvalid,
                              "TLS SNI setup failed");
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
      const bool verify_failed =
          tls_.verify_peer &&
          SSL_get_verify_result(secure_->next_layer().native_handle()) !=
              X509_V_OK;
      DropConnection();
      if (verify_failed)
        co_return hquant::Error(ErrorCode::kNetTlsVerifyFailed, ec.message());
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
    co_return hquant::Error(ErrorCode::kNetBusy,
                            "concurrent WebSocket operation");
  DropConnection();
  co_return co_await Connect(deadline);
}

asio::awaitable<absl::Status> WebSocketClient::WriteText(
    std::string message, std::chrono::steady_clock::time_point deadline) {
  if (!connected_ || in_flight_) {
    co_return hquant::Error(ErrorCode::kNetBusy,
                            "WebSocket unavailable or busy");
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
    co_return hquant::Error(ErrorCode::kNetBusy,
                            "WebSocket unavailable or busy");
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
  if (in_flight_)
    co_return hquant::Error(ErrorCode::kNetBusy, "WebSocket busy");
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

}  // namespace hquant
