#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/shard/shard.h"
#include "hquant/sqlite_history.h"

namespace hquant::v1 {

struct ControlHandlers {
  std::function<std::vector<ShardStatus>()> status;
  std::function<HistoryHealth()> history_health;
  std::function<absl::StatusOr<HistoryPage>(uint32_t, uint64_t)> history;
  std::function<absl::StatusOr<StrategyUpdateResult>(
      ShardId, uint64_t, StrategyPatch)> set_strategy;
  std::function<void()> stop;
};

class ControlServer {
 public:
  ControlServer(std::string socket_path, ControlHandlers handlers);
  ~ControlServer();
  ControlServer(const ControlServer&) = delete;
  ControlServer& operator=(const ControlServer&) = delete;

  absl::Status Start();
  void Stop();

 private:
  void Run();
  void ServeClient(int client_fd);
  std::string HandleLine(std::string_view line);

  std::string socket_path_;
  ControlHandlers handlers_;
  std::atomic<bool> stopping_{false};
  int listen_fd_ = -1;
  std::thread thread_;
};

}  // namespace hquant::v1
