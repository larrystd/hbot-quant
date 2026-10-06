#include "hquant/runtime.h"

#include <algorithm>
#include <chrono>
#include <exception>
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
                            std::chrono::system_clock::now().time_since_epoch())
                            .count());
}

}  // namespace

Runtime::Runtime(const AppConfig& config, PerfProbe* perf)
    : config_(config),
      run_id_(NewRunId()),
      perf_(perf),
      // A synchronized refresh across 1000 shards can create more than 8192
      // command, report, and balance records in one short burst.
      history_(config_.sqlite_path,
               std::max<size_t>(8192, config_.shards.size() * 64),
               perf ? &perf->history : nullptr,
               config_.input.source == InputSource::Replay),
      control_(
          config_.control_socket,
          ControlHandlers{
              [this] { return Status(); }, [this] { return history_.Health(); },
              [this](uint32_t limit, uint64_t cursor) {
                return history_.Query(limit, cursor);
              },
              [this](ShardId id, uint64_t version, StrategyPatch patch) {
                return UpdateStrategy(id, version, std::move(patch));
              },
              [this] { RequestStop(); }}) {
  const size_t worker_count = std::min<size_t>(8, config_.shards.size());
  io_contexts_.reserve(worker_count);
  for (size_t i = 0; i < worker_count; ++i) {
    io_contexts_.push_back(std::make_unique<boost::asio::io_context>());
  }
  shards_.reserve(config_.shards.size());
  for (size_t i = 0; i < config_.shards.size(); ++i) {
    const ShardConfig& shard = config_.shards[i];
    shards_.push_back(std::make_unique<Shard>(
        *io_contexts_[i % worker_count], shard, config_.input, history_, run_id_,
        perf_ ? &perf_->shards[shard.id.value] : nullptr));
    shard_by_id_[shard.id.value] = shards_.back().get();
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
  try {
    for (const auto& io : io_contexts_) {
      workers_.emplace_back([context = io.get()] { context->run(); });
    }
  } catch (const std::exception& error) {
    RequestStop();
    (void)Wait();
    return absl::ResourceExhaustedError(
        std::string("cannot start runtime worker: ") + error.what());
  }
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
  return ReadReplayFile(
      config_.input.replay_file, config_,
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
  for (std::thread& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
  for (const auto& shard : shards_) shard->Join();
  const absl::Status history_status = history_.FlushAndStop();
  control_.Stop();
  waited_ = true;
  return history_status;
}

bool Runtime::PostReplay(ReplayRecord record,
                         std::function<void(absl::Status)> done) {
  if (record.target.value < kMaxShards &&
      shard_by_id_[record.target.value]) {
    shard_by_id_[record.target.value]->PostReplay(std::move(record),
                                                   std::move(done));
    return true;
  }
  done(absl::NotFoundError("replay target shard not found"));
  return false;
}

bool Runtime::PostStatus(ShardId id, std::function<void(ShardStatus)> done) {
  if (id.value < kMaxShards && shard_by_id_[id.value]) {
    shard_by_id_[id.value]->PostStatus(std::move(done));
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
    shard->PostStatus(
        [done](ShardStatus status) { done->set_value(std::move(status)); });
    result.push_back(future.get());
  }
  return result;
}

absl::StatusOr<StrategyUpdateResult> Runtime::UpdateStrategy(
    ShardId id, uint64_t expected_version, StrategyPatch patch) {
  if (id.value < kMaxShards && shard_by_id_[id.value]) {
    auto done =
        std::make_shared<std::promise<absl::StatusOr<StrategyUpdateResult>>>();
    auto future = done->get_future();
    shard_by_id_[id.value]->PostStrategyUpdate(
        expected_version, std::move(patch),
        [done](absl::StatusOr<StrategyUpdateResult> result) {
          done->set_value(std::move(result));
        });
    return future.get();
  }
  return absl::NotFoundError("strategy shard not found");
}

}  // namespace hquant::v1
