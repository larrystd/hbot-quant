#pragma once

#include <atomic>
#include <concepts>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "boost/asio/executor_work_guard.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/steady_timer.hpp"
#include "hquant/config.h"
#include "hquant/perf_probe.h"
#include "hquant/shard/executor.h"
#include "hquant/shard/market.h"
#include "hquant/shard/market_type.h"
#include "hquant/shard/paper_exchange.h"
#include "hquant/shard/strategy.h"
#include "hquant/sqlite_history.h"

namespace hquant::v1 {

// 策略的固定接口：输入盘口视图、账户视图和当前时间，输出一个决策。
template <typename S>
concept DecisionStrategy =
    requires(S& strategy, const BookView& book, const ExecutionView& execution,
             MonoTime now) {
      {
        strategy.Decide(book, execution, now)
      } -> std::same_as<absl::StatusOr<StrategyDecision>>;
    };

// 运行一次决策并交给 executor 执行。决策一次只能表达一种动作，所以若第一次
// 是撤单且盘口仍 Live，就用撤单后的账户视图（资金已解冻）再决策一次，把新单
// 挂上。每批执行结果在执行后立即交给 record，前一批不会因后一批出错而丢失。
// 返回两批命令的总数。
template <DecisionStrategy S, typename Record>
  requires std::invocable<Record&, const std::vector<CommandResult>&>
absl::StatusOr<uint64_t> RunDecisionCycle(S& strategy, Executor& executor,
                                          const BookView& book, MonoTime now,
                                          Record&& record,
                                          LiveHandlerPhases* phases = nullptr) {
  auto account_view = [&]() {
    BenchPhaseTimer timer(phases ? &phases->account_view_ns : nullptr);
    return executor.View();
  };
  auto decide = [&]() {
    auto view = account_view();
    BenchPhaseTimer timer(phases ? &phases->strategy_ns : nullptr);
    return strategy.Decide(book, view, now);
  };
  auto dispatch = [&](const StrategyDecision& decision) {
    BenchPhaseTimer timer(phases ? &phases->executor_ns : nullptr);
    return executor.Dispatch(decision, book);
  };
  auto record_commands = [&](const std::vector<CommandResult>& commands) {
    BenchPhaseTimer timer(phases ? &phases->command_history_ns : nullptr);
    record(commands);
  };
  auto decision = decide();
  if (!decision.ok()) return decision.status();
  const bool cancelled = std::holds_alternative<CancelOrders>(*decision);
  auto first = dispatch(*decision);
  if (!first.ok()) return first.status();
  record_commands(*first);
  const uint64_t first_count = first->size();
  if (!cancelled || book.state != BookState::Live) return first_count;

  if (!account_view().active_order_ids.empty()) {
    return absl::InternalError("cancel left active orders");
  }
  auto second = decide();
  if (!second.ok()) return second.status();
  if (std::holds_alternative<CancelOrders>(*second)) {
    return absl::InternalError("second decision requested another cancel");
  }
  auto executed = dispatch(*second);
  if (!executed.ok()) return executed.status();
  record_commands(*executed);
  return first_count + executed->size();
}

struct TimerTick {};

struct ReplayRecord {
  ShardId target;
  InputTime time;
  std::variant<MarketInput, TimerTick> body;
  uint64_t perf_post_ns = 0;
};

struct StrategyPatch {
  std::optional<uint64_t> order_amount_nanos;
  std::optional<uint64_t> bid_spread_ppb;
  std::optional<uint64_t> ask_spread_ppb;
  std::optional<int64_t> refresh_ms;
};

struct StrategyUpdateResult {
  uint64_t config_version = 0;
  uint64_t actions = 0;
};

struct ShardStatus {
  ShardId id;
  bool stop_requested = false;
  bool worker_exited = false;
  BookState book_state = BookState::Syncing;
  uint64_t connection_epoch = 0;
  uint64_t last_book_sequence = 0;
  uint64_t applied_depth_events = 0;
  uint64_t accepted_trades = 0;
  uint64_t last_trade_id = 0;
  uint64_t depth_digest = 0;
  uint64_t trade_digest = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
  uint64_t market_notices = 0;
  uint64_t strategy_timer_ticks = 0;
  uint64_t config_version = 1;
  size_t active_orders = 0;
  uint64_t base_total_nanos = 0;
  uint64_t base_available_nanos = 0;
  uint64_t quote_total_nanos = 0;
  uint64_t quote_available_nanos = 0;
  std::string last_error;
};

class Shard {
 public:
  Shard(boost::asio::io_context& io, const ShardConfig& config,
        const InputConfig& input,
        SqliteHistory& history, std::string run_id, ShardPerf* perf = nullptr);
  ~Shard();
  Shard(const Shard&) = delete;
  Shard& operator=(const Shard&) = delete;

  absl::Status Start();
  void PostReplay(ReplayRecord record, std::function<void(absl::Status)> done);
  void PostStatus(std::function<void(ShardStatus)> done);
  void PostStrategyUpdate(
      uint64_t expected_version, StrategyPatch patch,
      std::function<void(absl::StatusOr<StrategyUpdateResult>)> done);
  void RequestStop();
  void Join();
  ShardId Id() const { return config_.id; }

 private:
  void OnMarket(const MarketNotice& notice);
  void OnStrategyTimer();
  void ScheduleStrategyTimer();
  void StopOnThread();
  void StopFromError(absl::Status error);
  absl::StatusOr<uint64_t> RunStrategyDecision(
      LiveHandlerPhases* phases = nullptr);
  void RecordCommands(const std::vector<CommandResult>& commands);
  void OnAccountEvent(const AccountEvent& event);
  void Record(HistoryRecord record);
  int64_t EventAtUs() const;
  ShardStatus CaptureStatus() const;

  ShardConfig config_;
  InputConfig input_;
  SqliteHistory& history_;
  std::string run_id_;
  boost::asio::io_context& io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      work_guard_;
  Market market_;
  Strategy strategy_;
  PaperExchange exchange_;  // must precede executor_, which references it
  Executor executor_;
  boost::asio::steady_timer strategy_timer_;
  std::atomic<bool> started_{false};
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> worker_exited_{false};
  bool stopped_on_thread_ = false;
  std::optional<InputTime> last_input_time_;
  uint64_t market_notices_ = 0;
  uint64_t strategy_timer_ticks_ = 0;
  uint64_t config_version_ = 1;
  uint64_t history_sequence_ = 0;
  std::string last_error_;
  std::optional<absl::Status> fatal_status_;
  ShardPerf* perf_ = nullptr;
  uint64_t current_input_origin_ns_ = 0;
};

}  // namespace hquant::v1
