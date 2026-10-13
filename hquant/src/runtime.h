#pragma once

#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "boost/asio/io_context.hpp"
#include "hquant/base/ids.h"
#include "hquant/config.h"
#include "hquant/control_server.h"
#include "hquant/perf_probe.h"
#include "hquant/shard/shard.h"
#include "hquant/sqlite_history.h"
#include "hquant/trading/trading_account.h"
#include "hquant/trading/order_journal.h"

namespace hquant::v1 {

class Runtime {
 public:
  explicit Runtime(const AppConfig& config, PerfProbe* perf = nullptr);
  ~Runtime();
  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  absl::Status Start();
  absl::Status RunReplay(const std::function<bool()>& interrupted);
  void RequestStop();
  absl::Status Wait();
  bool StopRequested() const { return stop_requested_; }
  const std::vector<AccountStatus>& FinalAccountStatuses() const {
    return final_account_statuses_;
  }
  bool PostReplay(ReplayRecord record, std::function<void(absl::Status)> done);
  bool PostStatus(ShardId id, std::function<void(ShardStatus)> done);

 private:
  std::vector<ShardStatus> Status();
  std::vector<AccountStatus> AccountStatuses();
  absl::StatusOr<StrategyUpdateResult> UpdateStrategy(ShardId id,
                                                      uint64_t expected_version,
                                                      StrategyPatch patch);
  absl::StatusOr<uint64_t> RunAccountAction(std::string account_id,
                                             AccountAction action,
                                             std::string operator_id,
                                             std::string reason);
  absl::StatusOr<JournalControlPage> AccountControls(
      std::string account_id, uint32_t limit, uint64_t cursor);

  AppConfig config_;
  std::string run_id_;
  PerfProbe* perf_ = nullptr;
  SqliteHistory history_;
  OrderJournal journal_;
  ControlServer control_;
  std::vector<std::unique_ptr<boost::asio::io_context>> io_contexts_;
  std::vector<std::thread> workers_;
  size_t account_worker_count_ = 0;
  size_t shard_worker_count_ = 0;
  std::map<std::string, std::unique_ptr<TradingAccount>> accounts_;
  std::vector<AccountStatus> final_account_statuses_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::array<Shard*, kMaxShards> shard_by_id_{};
  std::array<size_t, kMaxShards> shard_worker_by_id_{};
  std::atomic<bool> stop_requested_{false};
  bool started_ = false;
  bool waited_ = false;
  bool journal_started_ = false;
};

}  // namespace hquant::v1
