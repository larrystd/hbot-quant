#include "hquant/runtime.h"

#include <time.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <stdexcept>
#include <utility>

#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/mach_time.h>
#include <mach/thread_policy.h>
#include <pthread/qos.h>
#endif

#include "absl/status/status.h"
#include "hquant/replay_reader.h"
#include "hquant/venue/binance/trade_client.h"

namespace hquant::v1 {
namespace {

std::string NewRunId() {
  return std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count());
}

uint64_t ThreadCpuNs() {
  timespec now{};
  if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &now) != 0) return 0;
  return static_cast<uint64_t>(now.tv_sec) * 1'000'000'000ULL + now.tv_nsec;
}

void RunAccountContext(boost::asio::io_context& context) {
  // After an account event, nearby Shards often submit another order within
  // tens of microseconds. Poll briefly before sleeping so their posts do not
  // pay a second OS wakeup. Account state is protected when Shards submit directly.
  constexpr auto kIdlePoll = std::chrono::microseconds(50);
  while (context.run_one() != 0) {
    auto idle_until = std::chrono::steady_clock::now() + kIdlePoll;
    while (!context.stopped() &&
           std::chrono::steady_clock::now() < idle_until) {
      if (context.poll_one() != 0)
        idle_until = std::chrono::steady_clock::now() + kIdlePoll;
    }
  }
}

#ifdef __APPLE__
int SetRealtimeIoPolicy(bool* verified) {
  mach_timebase_info_data_t timebase{};
  const kern_return_t timebase_status = ::mach_timebase_info(&timebase);
  if (timebase_status != KERN_SUCCESS) return timebase_status;
  const auto ticks = [&timebase](uint64_t nanoseconds) {
    return static_cast<uint32_t>(static_cast<__uint128_t>(nanoseconds) *
                                 timebase.denom / timebase.numer);
  };
  // Benchmark only: nominally 100 us of computation in each 1 ms period.
  thread_time_constraint_policy_data_t policy{ticks(1'000'000), ticks(100'000),
                                              ticks(200'000), TRUE};
  const thread_act_t thread = ::pthread_mach_thread_np(::pthread_self());
  const kern_return_t status =
      ::thread_policy_set(thread, THREAD_TIME_CONSTRAINT_POLICY,
                          reinterpret_cast<thread_policy_t>(&policy),
                          THREAD_TIME_CONSTRAINT_POLICY_COUNT);
  if (status != KERN_SUCCESS) return status;
  thread_time_constraint_policy_data_t actual{};
  mach_msg_type_number_t count = THREAD_TIME_CONSTRAINT_POLICY_COUNT;
  boolean_t is_default = FALSE;
  const kern_return_t read_status = ::thread_policy_get(
      thread, THREAD_TIME_CONSTRAINT_POLICY,
      reinterpret_cast<thread_policy_t>(&actual), &count, &is_default);
  *verified = read_status == KERN_SUCCESS && !is_default &&
              count == THREAD_TIME_CONSTRAINT_POLICY_COUNT &&
              actual.period == policy.period &&
              actual.computation == policy.computation &&
              actual.constraint == policy.constraint;
  return status;
}
#endif

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
      journal_((std::filesystem::path(config_.sqlite_path).parent_path() /
                "order_journal.sqlite")
                   .string()),
      control_(
          config_.control_socket,
          ControlHandlers{
              [this] { return Status(); }, [this] { return AccountStatuses(); },
              [this] { return history_.Health(); },
              [this](uint32_t limit, uint64_t cursor) {
                return history_.Query(limit, cursor);
              },
              [this](ShardId id, uint64_t version, StrategyPatch patch) {
                return UpdateStrategy(id, version, std::move(patch));
              },
              [this](std::string account_id, AccountAction action,
                     std::string operator_id, std::string reason) {
                return RunAccountAction(std::move(account_id), action,
                                        std::move(operator_id),
                                        std::move(reason));
              },
              [this](std::string account_id, uint32_t limit, uint64_t cursor) {
                return AccountControls(std::move(account_id), limit, cursor);
              },
              [this] { RequestStop(); }}) {
  account_worker_count_ = std::min<size_t>(2, config_.accounts.size());
  shard_worker_count_ = std::min<size_t>(8, config_.shards.size());
  auto allocations = ResolveShardAllocations(config_);
  if (!allocations.ok()) {
    throw std::invalid_argument(std::string(allocations.status().message()));
  }
  io_contexts_.reserve(account_worker_count_ + shard_worker_count_);
  for (size_t i = 0; i < account_worker_count_ + shard_worker_count_; ++i) {
    io_contexts_.push_back(std::make_unique<boost::asio::io_context>());
  }
  size_t account_index = 0;
  for (const AccountConfig& config : config_.accounts) {
    std::vector<ShardConfig> members;
    for (const ShardConfig& shard : config_.shards) {
      if (shard.account_id == config.id) members.push_back(shard);
    }
    auto account = std::make_unique<TradingAccount>(
        *io_contexts_[account_index++ % account_worker_count_], config,
        std::move(members), run_id_, &history_, 8192,
        perf_ ? &perf_->trade : nullptr);
    accounts_.emplace(config.id, std::move(account));
  }
  shards_.reserve(config_.shards.size());
  std::map<std::string, size_t> next_worker_for_account;
  for (size_t i = 0; i < config_.shards.size(); ++i) {
    const ShardConfig& shard = config_.shards[i];
    TradingAccount* account = accounts_.at(shard.account_id).get();
    const auto account_config = std::find_if(
        config_.accounts.begin(), config_.accounts.end(),
        [&](const AccountConfig& value) { return value.id == shard.account_id; });
    const size_t available_workers =
        std::min(shard_worker_count_, account_config->rest_connections);
    const size_t shard_worker =
        next_worker_for_account[shard.account_id]++ % available_workers;
    shard_worker_by_id_[shard.id.value] = shard_worker;
    shards_.push_back(std::make_unique<Shard>(
        *io_contexts_[account_worker_count_ + shard_worker], shard,
        config_.input, *account_config, allocations->at(shard.id.value),
        perf_ ? &perf_->shards[shard.id.value] : nullptr, account));
    shard_by_id_[shard.id.value] = shards_.back().get();
    Shard* target = shards_.back().get();
    account->SetViewSink(shard.id, [target](AccountView view) {
      target->PostAccountView(std::move(view));
    });
    account->SetTradeSink(
        shard.id,
        TradingAccount::TradeSink{
            [target](AccountEvent event) {
              target->PostTradeEvent(std::move(event));
            },
            [target](ClientOrderId id, bool cancel, TradeResponse response) {
              target->PostTradeResponse(std::move(id), cancel,
                                        std::move(response));
            },
            [target](ClientOrderId id, bool cancel) {
              target->PostTradeWriting(std::move(id), cancel);
            },
            [target](std::function<void(uint64_t)> done) {
              target->PostCancelAll(std::move(done));
            }});
  }
}

Runtime::~Runtime() {
  RequestStop();
  (void)Wait();
}

absl::Status Runtime::Start() {
  if (started_) return absl::FailedPreconditionError("runtime already started");
  if (auto status = journal_.Start(); !status.ok()) return status;
  journal_started_ = true;
  for (const AccountConfig& config : config_.accounts) {
    auto* account = accounts_.at(config.id).get();
    account->SetJournal(&journal_);
    auto entries = journal_.Load(config.id);
    if (!entries.ok()) return entries.status();
    if (auto status = account->RestoreJournal(*entries); !status.ok()) {
      return status;
    }
    for (const ShardConfig& shard : config_.shards) {
      if (shard.account_id != config.id) continue;
      if (auto status = shard_by_id_[shard.id.value]->RestoreTradingJournal(
              *entries);
          !status.ok()) {
        return status;
      }
    }
  }
  // All Binance accounts in this process share one outbound request budget.
  // If account configs differ, the smallest nonzero limit is conservative.
  uint64_t shared_weight_limit = 0;
  uint64_t shared_order_limit = 0;
  for (const AccountConfig& account : config_.accounts) {
    if (account.mode != TradingMode::PressureSim &&
        account.mode != TradingMode::OnlineSim)
      continue;
    if (account.request_weight_per_minute != 0) {
      shared_weight_limit = shared_weight_limit == 0
                                ? account.request_weight_per_minute
                                : std::min(shared_weight_limit,
                                           account.request_weight_per_minute);
    }
    if (account.order_placements_per_ten_seconds != 0) {
      shared_order_limit =
          shared_order_limit == 0
              ? account.order_placements_per_ten_seconds
              : std::min(shared_order_limit,
                         account.order_placements_per_ten_seconds);
    }
  }
  auto request_scheduler = std::make_shared<RequestScheduler>(
      shared_weight_limit, shared_order_limit);
  size_t account_index = 0;
  for (const AccountConfig& config : config_.accounts) {
    const size_t worker = account_index++ % account_worker_count_;
    if (config.mode != TradingMode::PressureSim &&
        config.mode != TradingMode::OnlineSim)
      continue;
    std::string key = "hquant-sim-key";
    std::string secret = "hquant-sim-secret";
    if (config.mode == TradingMode::OnlineSim) {
      auto credentials = LoadApiCredentialsFile(config.api_credentials_file);
      if (!credentials.ok()) return credentials.status();
      key = std::move(credentials->key);
      secret = std::move(credentials->secret);
    } else if (!config.api_key_env.empty()) {
      const char* value = std::getenv(config.api_key_env.c_str());
      if (!value || !*value) {
        return absl::InvalidArgumentError(
            "trading API key environment missing");
      }
      key = value;
    }
    if (config.mode != TradingMode::OnlineSim &&
        !config.api_secret_env.empty()) {
      const char* value = std::getenv(config.api_secret_env.c_str());
      if (!value || !*value) {
        return absl::InvalidArgumentError(
            "trading API secret environment missing");
      }
      secret = value;
    }
    TradingAccount* account = accounts_.at(config.id).get();
    std::vector<size_t> shard_workers;
    for (const ShardConfig& shard : config_.shards) {
      if (shard.account_id == config.id)
        shard_workers.push_back(shard_worker_by_id_[shard.id.value]);
    }
    std::sort(shard_workers.begin(), shard_workers.end());
    shard_workers.erase(
        std::unique(shard_workers.begin(), shard_workers.end()),
        shard_workers.end());
    const auto on_event = [account](AccountEvent event) {
      account->OnVenueEvent(std::move(event));
    };
    const auto on_health = [account](bool connected, std::string reason) {
      account->OnConnectionState(connected, std::move(reason));
    };
    const std::string rule_symbol =
        config.mode == TradingMode::OnlineSim
            ? config_.shards.front().market.id.symbol
            : std::string{};
    if (shard_workers.empty()) {
      account->SetPort(std::make_unique<BinanceTradeClient>(
          *io_contexts_[worker], config, key, secret, on_event, on_health,
          perf_ ? &perf_->trade : nullptr, 8192, request_scheduler,
          rule_symbol));
      continue;
    }
    for (size_t index = 0; index < shard_workers.size(); ++index) {
      const size_t shard_worker = shard_workers[index];
      AccountConfig port_config = config;
      port_config.rest_connections =
          config.rest_connections / shard_workers.size() +
          (index < config.rest_connections % shard_workers.size() ? 1 : 0);
      auto client = std::make_unique<BinanceTradeClient>(
          *io_contexts_[worker], port_config, key, secret, on_event, on_health,
          perf_ ? &perf_->trade : nullptr, 8192, request_scheduler,
          index == 0 ? rule_symbol : std::string{},
          io_contexts_[account_worker_count_ + shard_worker].get(),
          index == 0);
      TradePort* port = client.get();
      if (index == 0)
        account->SetPort(std::move(client));
      else
        account->AddShardPort(std::move(client));
      for (const ShardConfig& shard : config_.shards) {
        if (shard.account_id == config.id &&
            shard_worker_by_id_[shard.id.value] == shard_worker)
          account->SetShardPort(shard.id, port);
      }
    }
  }
  if (auto status = history_.Start(); !status.ok()) return status;
  started_ = true;
  const char* qos_env = std::getenv("HQUANT_BENCH_PERFORMANCE_QOS");
  const bool prefer_performance_qos =
      perf_ && (!qos_env || std::strcmp(qos_env, "0") != 0);
  const char* realtime_env = std::getenv("HQUANT_BENCH_REALTIME_IO");
  const bool realtime_io =
      perf_ && (!realtime_env || std::strcmp(realtime_env, "0") != 0);
  if (perf_) {
    perf_->io_worker_count = io_contexts_.size();
    perf_->performance_qos_requested = prefer_performance_qos;
    perf_->realtime_io_requested = realtime_io;
  }
  try {
    for (size_t index = 0; index < io_contexts_.size(); ++index) {
      workers_.emplace_back([context = io_contexts_[index].get(), perf = perf_,
                             index, account_worker_count = account_worker_count_,
                             prefer_performance_qos, realtime_io] {
        const uint64_t wall_start_ns = BenchClockNowNs();
        const uint64_t cpu_start_ns = ThreadCpuNs();
        int qos_status = 0;
        if (prefer_performance_qos) {
#ifdef __APPLE__
          qos_status =
              ::pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#else
          qos_status = -1;
#endif
        }
        if (perf) perf->io_worker_qos_status[index] = qos_status;
        if (realtime_io) {
          bool verified = false;
#ifdef __APPLE__
          const int status = SetRealtimeIoPolicy(&verified);
#else
          const int status = -1;
#endif
          if (perf) {
            perf->io_worker_realtime_status[index] = status;
            perf->io_worker_realtime_verified[index] = verified;
          }
        }
        if (index < account_worker_count)
          RunAccountContext(*context);
        else
          context->run();
        if (perf) {
          perf->io_worker_cpu_ns[index] = ThreadCpuNs() - cpu_start_ns;
          perf->io_worker_wall_ns[index] = BenchClockNowNs() - wall_start_ns;
        }
      });
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
  for (const ShardConfig& shard : config_.shards) {
    TradingAccount* account = accounts_.at(shard.account_id).get();
    Shard* target = shard_by_id_[shard.id.value];
    if (!account->PostView(shard.id, [target](AccountView view) {
          target->PostAccountView(std::move(view));
        })) {
      RequestStop();
      (void)Wait();
      return absl::ResourceExhaustedError("account startup queue full");
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
  if (perf_) perf_->history.stop_started.store(true, std::memory_order_release);
  for (const auto& [id, account] : accounts_) account->PrepareStop();
  for (const auto& shard : shards_) shard->RequestStop();
}

absl::Status Runtime::Wait() {
  if (waited_) return absl::OkStatus();
  if (started_) RequestStop();
  if (workers_.size() == io_contexts_.size()) {
    for (const auto& shard : shards_) {
      auto done = std::make_shared<std::promise<void>>();
      auto future = done->get_future();
      shard->PostStopped([done] { done->set_value(); });
      future.wait();
    }
    final_account_statuses_.clear();
    for (const auto& [id, account] : accounts_) {
      auto done = std::make_shared<std::promise<AccountStatus>>();
      auto future = done->get_future();
      if (!account->PostFinalStatus([done](AccountStatus status) {
            done->set_value(std::move(status));
          })) {
        AccountStatus status;
        status.id = id;
        status.ready = false;
        status.error = "final account status queue unavailable";
        final_account_statuses_.push_back(std::move(status));
        continue;
      }
      final_account_statuses_.push_back(future.get());
    }
    for (auto& [id, account] : accounts_) account->RequestStop();
  } else {
    for (auto& context : io_contexts_) context->stop();
  }
  for (std::thread& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
  for (const auto& shard : shards_) shard->Join();
  const absl::Status history_status = history_.FlushAndStop();
  const absl::Status journal_status =
      journal_started_ ? journal_.FlushAndStop() : absl::OkStatus();
  control_.Stop();
  waited_ = true;
  return history_status.ok() ? journal_status : history_status;
}

bool Runtime::PostReplay(ReplayRecord record,
                         std::function<void(absl::Status)> done) {
  if (record.target.value < kMaxShards && shard_by_id_[record.target.value]) {
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

std::vector<AccountStatus> Runtime::AccountStatuses() {
  std::vector<AccountStatus> result;
  result.reserve(accounts_.size());
  for (const auto& [id, account] : accounts_) {
    auto done = std::make_shared<std::promise<AccountStatus>>();
    auto future = done->get_future();
    if (!account->PostStatus([done](AccountStatus status) {
          done->set_value(std::move(status));
        })) {
      AccountStatus status;
      status.id = id;
      status.ready = false;
      status.error = "account status queue unavailable";
      result.push_back(std::move(status));
      continue;
    }
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

absl::StatusOr<uint64_t> Runtime::RunAccountAction(std::string account_id,
                                                   AccountAction action,
                                                   std::string operator_id,
                                                   std::string reason) {
  if (stop_requested_) {
    return absl::FailedPreconditionError("runtime is stopping");
  }
  const auto found = accounts_.find(account_id);
  if (found == accounts_.end()) {
    return absl::NotFoundError("account not found");
  }
  auto done = std::make_shared<std::promise<absl::StatusOr<uint64_t>>>();
  auto future = done->get_future();
  if (!found->second->PostControl(action, std::move(operator_id),
                                  std::move(reason),
                                  [done](absl::StatusOr<uint64_t> result) {
                                    done->set_value(std::move(result));
                                  })) {
    return absl::ResourceExhaustedError("account control queue unavailable");
  }
  return future.get();
}

absl::StatusOr<JournalControlPage> Runtime::AccountControls(
    std::string account_id, uint32_t limit, uint64_t cursor) {
  if (!accounts_.contains(account_id)) {
    return absl::NotFoundError("account not found");
  }
  if (!journal_started_) {
    return absl::FailedPreconditionError("account audit journal unavailable");
  }
  return journal_.QueryControls(account_id, limit, cursor);
}

}  // namespace hquant::v1
