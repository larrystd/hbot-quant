#pragma once

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "boost/asio.hpp"
#include "boost/asio/ssl.hpp"
#include "boost/beast/core.hpp"
#include "boost/beast/ssl.hpp"
#include "hbot/net/api.h"
#include "hbot/net/tls_config.h"

namespace hbot {

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

}  // namespace hbot
