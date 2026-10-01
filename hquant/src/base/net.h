#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "boost/asio.hpp"
#include "boost/asio/awaitable.hpp"
#include "boost/asio/ssl.hpp"
#include "boost/asio/ssl/context.hpp"
#include "boost/beast/core.hpp"
#include "boost/beast/ssl.hpp"
#include "boost/beast/websocket.hpp"

namespace hquant {

struct TlsConfig {
  bool enabled = false;
  bool verify_peer = true;
  std::string ca_file;
  std::string server_name;
};

// Verification is enabled by default. server_name overrides the endpoint host
// for both certificate hostname checking and SNI.
absl::Status ConfigureTlsContext(boost::asio::ssl::context& context,
                                 const TlsConfig& config);

struct HttpRequest {
  std::string method;
  std::string target;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
  std::chrono::steady_clock::time_point deadline;
};

struct HttpResponse {
  unsigned status = 0;
  std::string body;
  std::chrono::steady_clock::duration retry_after{};
  std::vector<std::pair<std::string, std::string>> headers;
};

class HttpTransport {
 public:
  virtual ~HttpTransport() = default;
  virtual boost::asio::awaitable<absl::StatusOr<HttpResponse>> Send(
      HttpRequest request) = 0;
};

// One client belongs to one io_context and permits one outstanding Send. A
// successful keep-alive response leaves the connection open for the next Send.
class HttpClient final : public HttpTransport {
 public:
  HttpClient(boost::asio::io_context& io, std::string host, std::string port,
             TlsConfig tls = {});

  boost::asio::awaitable<absl::StatusOr<HttpResponse>> Send(
      HttpRequest request) override;
  void Cancel();
  bool Connected() const;

 private:
  using Tcp = boost::asio::ip::tcp;
  using PlainStream = boost::beast::tcp_stream;
  using SecureStream = boost::beast::ssl_stream<PlainStream>;

  boost::asio::awaitable<absl::Status> Connect(
      std::chrono::steady_clock::time_point deadline);
  void DropConnection();

  boost::asio::io_context& io_;
  std::string host_;
  std::string port_;
  TlsConfig tls_;
  boost::asio::ssl::context ssl_context_;
  absl::Status tls_status_;
  Tcp::resolver resolver_;
  boost::asio::steady_timer resolver_deadline_;
  std::unique_ptr<PlainStream> plain_;
  std::unique_ptr<SecureStream> secure_;
  bool in_flight_ = false;
  bool cancelled_ = false;
};

// Single-owner WebSocket connection. Calls run on its io_context; a disconnect
// never silently resends application messages or invents a new subscription.
class WebSocketClient {
 public:
  WebSocketClient(boost::asio::io_context& io, std::string host,
                  std::string port, std::string target, TlsConfig tls = {});

  boost::asio::awaitable<absl::Status> Connect(
      std::chrono::steady_clock::time_point deadline);
  boost::asio::awaitable<absl::Status> Reconnect(
      std::chrono::steady_clock::time_point deadline);
  boost::asio::awaitable<absl::Status> WriteText(
      std::string message, std::chrono::steady_clock::time_point deadline);
  boost::asio::awaitable<absl::StatusOr<std::string>> Read(
      std::chrono::steady_clock::time_point deadline);
  boost::asio::awaitable<absl::Status> Close(
      std::chrono::steady_clock::time_point deadline);
  void Cancel();
  bool Connected() const;

 private:
  using Tcp = boost::asio::ip::tcp;
  using PlainStream = boost::beast::websocket::stream<boost::beast::tcp_stream>;
  using SecureStream = boost::beast::websocket::stream<
      boost::beast::ssl_stream<boost::beast::tcp_stream>>;

  void DropConnection();
  void ArmDeadline(std::chrono::steady_clock::time_point deadline);
  void DisarmDeadline();
  absl::Status Error(const boost::system::error_code& ec,
                     std::chrono::steady_clock::time_point deadline) const;

  boost::asio::io_context& io_;
  std::string host_;
  std::string port_;
  std::string target_;
  TlsConfig tls_;
  boost::asio::ssl::context ssl_context_;
  absl::Status tls_status_;
  Tcp::resolver resolver_;
  boost::asio::steady_timer resolver_deadline_;
  boost::asio::steady_timer operation_deadline_;
  std::unique_ptr<PlainStream> plain_;
  std::unique_ptr<SecureStream> secure_;
  bool connected_ = false;
  bool cancelled_ = false;
  bool in_flight_ = false;
};

}  // namespace hquant
