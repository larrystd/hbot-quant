#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "boost/asio.hpp"

namespace hquant {

struct LineEndpoint {
  enum class Kind { Unix, Tcp } kind = Kind::Unix;
  std::string address;
  uint16_t port = 0;

  static LineEndpoint Unix(std::string path) {
    return {Kind::Unix, std::move(path), 0};
  }
  static LineEndpoint Tcp(std::string host, uint16_t port) {
    return {Kind::Tcp, std::move(host), port};
  }
};

// One request and response per connection. Lines are passed without '\n'.
class LineClient {
 public:
  LineClient(boost::asio::io_context& io, LineEndpoint endpoint,
             size_t max_frame = 64 * 1024);
  boost::asio::awaitable<absl::Status> Connect(
      std::chrono::steady_clock::time_point deadline);
  boost::asio::awaitable<absl::Status> Send(
      std::string line, std::chrono::steady_clock::time_point deadline);
  boost::asio::awaitable<absl::StatusOr<std::string>> Receive(
      std::chrono::steady_clock::time_point deadline);
  void Close();

 private:
  boost::asio::io_context& io_;
  LineEndpoint endpoint_;
  size_t max_frame_;
  std::unique_ptr<boost::asio::local::stream_protocol::socket> unix_;
  std::unique_ptr<boost::asio::ip::tcp::socket> tcp_;
  bool in_flight_ = false;
};

class LineServer {
 public:
  struct Options {
    size_t max_sessions = 16;
    size_t max_frame = 64 * 1024;
    std::chrono::steady_clock::duration read_timeout =
        std::chrono::seconds(2);
    std::chrono::steady_clock::duration write_timeout =
        std::chrono::seconds(2);
    std::string busy_reply;
    std::string invalid_reply;
  };
  using Handler =
      std::function<boost::asio::awaitable<std::string>(std::string)>;

  static absl::StatusOr<std::unique_ptr<LineServer>> Start(
      boost::asio::io_context& io, LineEndpoint endpoint, Handler handler,
      Options options);
  ~LineServer();
  LineServer(const LineServer&) = delete;
  LineServer& operator=(const LineServer&) = delete;
  void Stop(bool remove_unix_path = true);
  uint16_t bound_port() const;

 private:
  struct State;
  explicit LineServer(std::shared_ptr<State> state);
  std::shared_ptr<State> state_;
};

}  // namespace hquant
