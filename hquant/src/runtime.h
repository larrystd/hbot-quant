#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/base/ids.h"
#include "hquant/config.h"
#include "hquant/control_server.h"
#include "hquant/shard/shard.h"
#include "hquant/sqlite_history.h"

namespace hquant::v1 {

class Runtime {
 public:
  explicit Runtime(const AppConfig& config);
  ~Runtime();
  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  absl::Status Start();
  absl::Status RunReplay(const std::function<bool()>& interrupted);
  void RequestStop();
  absl::Status Wait();
  bool StopRequested() const { return stop_requested_; }
  bool PostReplay(ReplayRecord record, std::function<void(absl::Status)> done);
  bool PostStatus(ShardId id, std::function<void(ShardStatus)> done);

 private:
  std::vector<ShardStatus> Status();
  absl::StatusOr<StrategyUpdateResult> UpdateStrategy(
      ShardId id, uint64_t expected_version, StrategyPatch patch);

  AppConfig config_;
  std::string run_id_;
  SqliteHistory history_;
  ControlServer control_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::atomic<bool> stop_requested_{false};
  bool started_ = false;
  bool waited_ = false;
};

}  // namespace hquant::v1
