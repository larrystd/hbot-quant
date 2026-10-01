#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"

namespace hquant {

struct StatusRequest {};
struct HistoryRequest {
  uint32_t limit = 20;
  std::string cursor;
};
struct StopRequest {};
using ControlRequestPayload =
    std::variant<StatusRequest, HistoryRequest, StopRequest>;

struct ControlRequest {
  uint32_t schema_version = 2;
  uint64_t request_id = 0;
  ControlRequestPayload payload;
};

struct StatusResponse {
  std::string json;
};
struct HistoryResponse {
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
    std::variant<StatusResponse, HistoryResponse, StopResponse, ControlError>;

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

// One JSON request and response line per Unix stream connection. Requests are
// handled on bounded workers so a history query cannot delay status or stop.
class ControlServer {
 public:
  using Handler = std::function<ControlResponse(const ControlRequest&)>;

  static absl::StatusOr<std::unique_ptr<ControlServer>> Start(
      std::string socket_path, Handler handler);
  ~ControlServer();
  ControlServer(const ControlServer&) = delete;
  ControlServer& operator=(const ControlServer&) = delete;

  void Stop();
  const std::string& socket_path() const { return socket_path_; }

 private:
  ControlServer(std::string path, Handler handler, int listen_fd);
  void AcceptLoop();
  void HandleConnection(int fd);

  struct Worker {
    std::thread thread;
    std::shared_ptr<std::atomic<bool>> done;
  };

  std::string socket_path_;
  Handler handler_;
  int listen_fd_ = -1;
  std::atomic<bool> stopping_{false};
  std::atomic<int> active_{0};
  std::thread accept_thread_;
  std::mutex workers_mutex_;
  std::vector<Worker> workers_;
};

class ShardRuntime;
class SimpleSimulatedExchange;
class SqliteHistoryWriter;
struct MarketSpec;
struct HistoryPage;

std::string EscapeJson(std::string_view text);
std::string StatusJson(const ShardRuntime& shard,
                       const SimpleSimulatedExchange& sim_exchange,
                       const MarketSpec& market,
                       const SqliteHistoryWriter& recorder);
std::string HistoryJson(const HistoryPage& page);

}  // namespace hquant
