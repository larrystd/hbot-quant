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
using ServerRequestPayload =
    std::variant<StatusRequest, HistoryRequest, StopRequest>;

struct ServerRequest {
  uint32_t schema_version = 2;
  uint64_t request_id = 0;
  ServerRequestPayload payload;
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
struct ServerError {
  ErrorCode code = ErrorCode::kInternal;
  std::string message;
};
using ServerResponsePayload =
    std::variant<StatusResponse, HistoryResponse, StopResponse, ServerError>;

struct ServerResponse {
  uint32_t schema_version = 2;
  uint64_t request_id = 0;
  ServerResponsePayload payload;
};

// One compact JSON object per frame. The Unix socket transport supplies frame
// boundaries; no C++ variant layout is sent over the wire.
absl::StatusOr<std::string> EncodeServerRequest(const ServerRequest& request);
absl::StatusOr<ServerRequest> DecodeServerRequest(std::string_view json);
absl::StatusOr<std::string> EncodeServerResponse(
    const ServerResponse& response);
absl::StatusOr<ServerResponse> DecodeServerResponse(std::string_view json);

// One JSON request and response line per Unix stream connection. Requests are
// handled on bounded workers so a history query cannot delay status or stop.
class QuantServer {
 public:
  using Handler = std::function<ServerResponse(const ServerRequest&)>;

  static absl::StatusOr<std::unique_ptr<QuantServer>> Start(
      std::string socket_path, Handler handler);
  ~QuantServer();
  QuantServer(const QuantServer&) = delete;
  QuantServer& operator=(const QuantServer&) = delete;

  void Stop();
  const std::string& socket_path() const { return socket_path_; }

 private:
  QuantServer(std::string path, Handler handler, int listen_fd);
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

class Shard;
class SimpleSimulatedExchange;
class SqliteHistoryWriter;
struct MarketSpec;
struct HistoryPage;

std::string EscapeJson(std::string_view text);
std::string StatusJson(const Shard& shard,
                       const SimpleSimulatedExchange& sim_exchange,
                       const MarketSpec& market,
                       const SqliteHistoryWriter& recorder);
std::string HistoryJson(const HistoryPage& page);

}  // namespace hquant
