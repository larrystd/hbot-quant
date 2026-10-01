#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "absl/status/statusor.h"
#include "base/net.h"
#include "boost/asio/awaitable.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/ip/tcp.hpp"
#include "boost/beast/http.hpp"

namespace hquant {

// Small plain-TCP Beast servers for local exchange emulation. The caller owns
// the io_context and stops it after closing the listener.
class WebSocketServer {
 public:
  using MessageSource =
      std::function<boost::asio::awaitable<std::optional<std::string>>(
          uint64_t session, uint64_t ordinal, std::string target)>;
  static boost::asio::awaitable<void> Serve(
      boost::asio::ip::tcp::socket socket,
      boost::beast::http::request<boost::beast::http::string_body> request,
      uint64_t session, MessageSource source);
};

class HttpServer {
 public:
  using Handler =
      std::function<HttpResponse(std::string method, std::string target)>;
  static absl::StatusOr<std::unique_ptr<HttpServer>> Start(
      boost::asio::io_context& io, std::string address, uint16_t port,
      Handler handler, WebSocketServer::MessageSource websocket);
  ~HttpServer();
  HttpServer(const HttpServer&) = delete;
  HttpServer& operator=(const HttpServer&) = delete;
  void Stop();
  uint16_t bound_port() const;

 private:
  struct State;
  explicit HttpServer(std::shared_ptr<State> state);
  std::shared_ptr<State> state_;
};

}  // namespace hquant
