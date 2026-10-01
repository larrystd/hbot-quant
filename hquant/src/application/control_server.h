#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <variant>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"
#include "base/line_stream.h"
#include "boost/asio/awaitable.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/signal_set.hpp"

namespace hquant {

struct StatusRequest {};
struct OrderHistoryRequest {
  uint32_t limit = 20;
  std::string cursor;
};
struct StopRequest {};
using ControlRequestPayload =
    std::variant<StatusRequest, OrderHistoryRequest, StopRequest>;

struct ControlRequest {
  uint32_t schema_version = 2;
  uint64_t request_id = 0;
  ControlRequestPayload payload;
};

struct StatusResponse {
  std::string json;
};
struct OrderHistoryResponse {
  std::string json;
};
struct StopResponse {
  bool accepted = false;
};
struct ControlError {
  ErrorCode code = ErrorCode::kInternal;
  std::string message;
};
using ControlResponsePayload =
    std::variant<StatusResponse, OrderHistoryResponse, StopResponse, ControlError>;

struct ControlResponse {
  uint32_t schema_version = 2;
  uint64_t request_id = 0;
  ControlResponsePayload payload;
};

// One compact JSON object per frame. The Unix socket transport supplies frame
// boundaries; no C++ variant layout is sent over the wire.
absl::StatusOr<std::string> EncodeControlRequest(const ControlRequest& request);
absl::StatusOr<ControlRequest> DecodeControlRequest(std::string_view json);
absl::StatusOr<std::string> EncodeControlResponse(
    const ControlResponse& response);
absl::StatusOr<ControlResponse> DecodeControlResponse(std::string_view json);

// One JSON request and response line per Unix stream connection. Sessions run
// as coroutines on the management io_context.
class ControlServer {
 public:
  using Handler = std::function<boost::asio::awaitable<ControlResponse>(
      ControlRequest)>;

  static absl::StatusOr<std::unique_ptr<ControlServer>> Start(
      std::string socket_path, Handler handler,
      std::function<void(int)> on_signal = {});
  ~ControlServer();
  ControlServer(const ControlServer&) = delete;
  ControlServer& operator=(const ControlServer&) = delete;

  // Stop accepting and drain workers. The owner may defer socket unlink until
  // the trading chain has finished shutting down.
  void Stop(bool remove_socket = true);
  const std::string& socket_path() const { return socket_path_; }

 private:
  ControlServer(std::string path, Handler handler);
  boost::asio::awaitable<std::string> HandleFrame(std::string frame);

  std::string socket_path_;
  Handler handler_;
  boost::asio::io_context io_;
  std::unique_ptr<boost::asio::signal_set> signals_;
  std::unique_ptr<LineServer> line_server_;
  std::thread thread_;
};

class Shard;
class SimpleSimulatedExchange;
class SqliteOrderHistoryWriter;
struct MarketSpec;
struct OrderHistoryPage;

std::string EscapeJson(std::string_view text);
std::string StatusJson(const Shard& shard,
                       const SimpleSimulatedExchange& sim_exchange,
                       const MarketSpec& market,
                       const SqliteOrderHistoryWriter& recorder);
std::string ShardStatusJson(const Shard& shard,
                            const SimpleSimulatedExchange& sim_exchange,
                            bool live);
std::string OrderHistoryJson(const OrderHistoryPage& page);

}  // namespace hquant
