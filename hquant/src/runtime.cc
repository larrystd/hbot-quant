#include "hquant/runtime.h"

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "hquant/replay_reader.h"

namespace hquant::v1 {
namespace {

std::string NewRunId() {
  return std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count());
}

}  // namespace

Runtime::Runtime(const AppConfig& config)
    : config_(config),
      run_id_(NewRunId()),
      history_(config_.sqlite_path),
      control_(config_.control_socket,
               ControlHandlers{
                   [this] { return Status(); },
                   [this] { return history_.Health(); },
                   [this](uint32_t limit, uint64_t cursor) {
                     return history_.Query(limit, cursor);
                   },
                   [this](ShardId id, uint64_t version, StrategyPatch patch) {
                     return UpdateStrategy(id, version, std::move(patch));
                   },
                   [this] { RequestStop(); }}) {
  shards_.reserve(config_.shards.size());
  for (const ShardConfig& shard : config_.shards) {
    shards_.push_back(std::make_unique<Shard>(shard, config_.input, history_, run_id_));
  }
}

Runtime::~Runtime() {
  RequestStop();
  (void)Wait();
}

absl::Status Runtime::Start() {
  if (started_) return absl::FailedPreconditionError("runtime already started");
  if (auto status = history_.Start(); !status.ok()) return status;
  started_ = true;
  for (const auto& shard : shards_) {
    if (auto status = shard->Start(); !status.ok()) {
      RequestStop();
      (void)Wait();
      return status;
    }
  }
  if (auto status = control_.Start(); !status.ok()) {
    RequestStop();
    (void)Wait();
    return status;
  }
  return absl::OkStatus();
}

absl::Status Runtime::RunReplay(const std::function<bool()>& interrupted) {
  if (!started_ || config_.input.source != InputSource::Replay) {
    return absl::FailedPreconditionError("runtime is not a started replay");
  }
  return ReadReplayFile(config_.input.replay_file, config_,
                        [this, &interrupted](ReplayRecord record) {
    if (stop_requested_ || interrupted()) {
      return absl::CancelledError("replay stopped");
    }
    auto done = std::make_shared<std::promise<absl::Status>>();
    auto future = done->get_future();
    PostReplay(std::move(record), [done](absl::Status status) {
      done->set_value(std::move(status));
    });
    return future.get();
  });
}

void Runtime::RequestStop() {
  if (stop_requested_.exchange(true)) return;
  for (const auto& shard : shards_) shard->RequestStop();
}

absl::Status Runtime::Wait() {
  if (waited_) return absl::OkStatus();
  if (started_) RequestStop();
  for (const auto& shard : shards_) shard->Join();
  const absl::Status history_status = history_.FlushAndStop();
  control_.Stop();
  waited_ = true;
  return history_status;
}

bool Runtime::PostReplay(ReplayRecord record,
                         std::function<void(absl::Status)> done) {
  for (const auto& shard : shards_) {
    if (shard->Id() != record.target) continue;
    shard->PostReplay(std::move(record), std::move(done));
    return true;
  }
  done(absl::NotFoundError("replay target shard not found"));
  return false;
}

bool Runtime::PostStatus(ShardId id,
                         std::function<void(ShardStatus)> done) {
  for (const auto& shard : shards_) {
    if (shard->Id() != id) continue;
    shard->PostStatus(std::move(done));
    return true;
  }
  return false;
}

std::vector<ShardStatus> Runtime::Status() {
  std::vector<ShardStatus> result;
  result.reserve(shards_.size());
  for (const auto& shard : shards_) {
    auto done = std::make_shared<std::promise<ShardStatus>>();
    auto future = done->get_future();
    shard->PostStatus([done](ShardStatus status) {
      done->set_value(std::move(status));
    });
    result.push_back(future.get());
  }
  return result;
}

absl::StatusOr<StrategyUpdateResult> Runtime::UpdateStrategy(
    ShardId id, uint64_t expected_version, StrategyPatch patch) {
  for (const auto& shard : shards_) {
    if (shard->Id() != id) continue;
    auto done = std::make_shared<
        std::promise<absl::StatusOr<StrategyUpdateResult>>>();
    auto future = done->get_future();
    shard->PostStrategyUpdate(expected_version, std::move(patch),
                              [done](absl::StatusOr<StrategyUpdateResult> result) {
      done->set_value(std::move(result));
    });
    return future.get();
  }
  return absl::NotFoundError("strategy shard not found");
}

}  // namespace hquant::v1
