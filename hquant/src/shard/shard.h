#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"
#include "base/market.h"
#include "base/order.h"
#include "base/types.h"
#include "boost/asio/awaitable.hpp"
#include "boost/asio/io_context.hpp"
#include "market/order_book.h"
#include "order/order_tracker.h"
#include "order_history/order_history.h"
#include "shard/action_executor.h"
#include "strategy/strategy.h"

namespace hquant {

class TradingMode;
class MarketDataReceiver;
class HttpClient;
class WebSocketClient;
namespace binance_spot {
class MarketDataStream;
}

// Owns both time domains used by the same deterministic input stream.
class ReplayClock final : public Clock {
 public:
  ReplayClock(UtcTime utc_origin, MonoTime mono_origin)
      : utc_origin_(utc_origin), mono_origin_(mono_origin) {}

  absl::Status Advance(InputTime stamp) {
    if (stamp.at_us < 0 || (last_ && (stamp.at_us < last_->at_us ||
                                      (stamp.at_us == last_->at_us &&
                                       stamp.ordinal <= last_->ordinal)))) {
      return Error(ErrorCode::kInputTimeInvalid,
                   "replay input order must increase");
    }
    last_ = stamp;
    return absl::OkStatus();
  }

  UtcTime UtcNow() const override {
    return utc_origin_ + std::chrono::microseconds(last_ ? last_->at_us : 0);
  }
  MonoTime MonoNow() const override {
    return mono_origin_ + std::chrono::microseconds(last_ ? last_->at_us : 0);
  }
  std::optional<InputTime> LastInput() const { return last_; }

 private:
  UtcTime utc_origin_;
  MonoTime mono_origin_;
  std::optional<InputTime> last_;
};

struct StopNewOrders {
  std::string reason;
};
struct CancelOwnedOrders {
  StrategyId strategy_id;
};
struct RequestShardReport {};
using ShardCommandPayload =
    std::variant<StopNewOrders, CancelOwnedOrders, RequestShardReport>;

struct ShardCommand {
  uint64_t command_id = 0;
  ShardId target_shard;
  ShardCommandPayload payload;
  UtcTime issued_at{};
  MonoTime deadline{};
};

struct ShardReport {
  ShardId shard;
  uint64_t report_version = 0;
  UtcTime observed_at{};
  std::map<std::string, Decimal> budget_usage_by_asset;
  std::map<std::string, bool> market_readiness;
  std::map<std::string, bool> account_freshness;
  std::map<std::string, uint64_t> queue_watermarks;
  OrderHistoryWriterHealth storage_health;
};

// G1 single-thread owner of strategy, book, trading mode, tracker and risk.
// The caller advances the clock and feeds inputs in InputTime order.
class Shard {
 public:
  struct FeedEndpoint {
    std::string host;
    uint16_t port = 443;
    bool tls = true;
  };
  struct Config {
    RunId run;
    ShardId shard;
    StrategyId strategy_id;
    AccountId account;
    MarketSpec market;
    TickLotSize scale;
    TradingRule rule;
    uint64_t stale_after_us = 60'000'000;
  };

  Shard(Config config, const Clock& clock, Strategy& strategy,
        std::unique_ptr<TradingMode> trading, RiskGate& risk,
        OrderHistoryWriter& recorder);
  Shard(Config config, const Clock& clock, std::unique_ptr<Strategy> strategy,
        std::unique_ptr<TradingMode> trading, std::unique_ptr<RiskGate> risk,
        OrderHistoryWriter& recorder);
  ~Shard();
  Shard(const Shard&) = delete;
  Shard& operator=(const Shard&) = delete;

  void StartFeed(FeedEndpoint rest, FeedEndpoint websocket);
  void RequestStop();
  void Join();
  boost::asio::io_context* LiveIo() const { return io_.get(); }
  uint64_t AppliedDiffs() const;
  uint64_t Resyncs() const;
  const absl::Status& stream_error() const { return stream_error_; }
  ShardId id() const { return config_.shard; }
  const MarketSpec& market() const { return config_.market; }

  BookApplyResult Subscribe(uint64_t connection_id);
  absl::Status OnSnapshot(const BookSnapshot& snapshot);
  absl::Status OnDiff(const BookDiff& diff);
  absl::Status OnPublicTrade(const PublicTrade& trade);
  // 实时行情的入口：StartFeed 把它交给行情模块。测试自己驱动行情模块时也用它。
  MarketDataReceiver& Receiver();
  absl::StatusOr<std::vector<ActionResult>> OnTimer(InputTime stamp);

  const OrderBookView& Book() const { return book_.View(); }
  const hquant::Order* FindOrder(const ClientOrderId& id) const {
    return tracker_.Find(id);
  }
  const std::vector<OrderHistoryGap>& local_gaps() const { return local_gaps_; }
  uint64_t shard_sequence() const { return shard_sequence_; }
  uint64_t strategy_invocations() const { return strategy_invocations_; }

 private:
  absl::Status AfterBookApply(const BookApplyResult& result);
  absl::Status RunPendingTrigger(std::optional<Trigger> explicit_trigger);
  absl::StatusOr<std::vector<ActionResult>> RunStrategy(
      Trigger why, InputTime stamp, bool allow_followup = true);
  template <typename Step>
  absl::Status RunStep(Step&& step);
  void OnReport(const AccountEvent& report);
  absl::Status ProcessQueuedReports();
  absl::Status ProcessAccountEvent(const AccountEvent& event);
  absl::Status Record(OrderHistoryRecordPayload payload,
                      const StrategyId& strategy_id);
  void AddGap(uint64_t sequence);
  absl::StatusOr<Decimal> Price(PriceTicks ticks) const;
  boost::asio::awaitable<void> TimerLoop();

  Config config_;
  std::unique_ptr<Strategy> owned_strategy_;
  std::unique_ptr<RiskGate> owned_risk_;
  const Clock& clock_;
  Strategy& strategy_;
  // 随运行模式不同的部分：模拟盘是 SimulatedTrading，实盘是 LiveTrading。
  std::unique_ptr<TradingMode> trading_;
  RiskGate& risk_;
  OrderHistoryWriter& recorder_;
  OrderBookSync book_;
  OrderTracker tracker_;
  uint64_t shard_sequence_ = 0;
  uint64_t next_action_batch_id_ = 1;
  ActionExecutor action_executor_;
  std::map<std::string, HoldId> holds_;
  std::vector<OrderHistoryGap> local_gaps_;
  std::optional<Decimal> last_trade_price_;
  MonoTime origin_mono_{};
  uint64_t event_ordinal_ = 0;
  bool in_strategy_ = false;
  // 正在执行步骤时到达的回报，步骤结束后处理。
  std::deque<AccountEvent> queued_reports_;
  bool step_running_ = false;
  uint64_t strategy_invocations_ = 0;
  std::optional<Trigger> pending_trigger_;
  std::optional<MonoTime> last_strategy_at_;
  class StreamReceiver;
  std::unique_ptr<StreamReceiver> receiver_;
  std::unique_ptr<boost::asio::io_context> io_;
  std::unique_ptr<HttpClient> http_;
  std::unique_ptr<WebSocketClient> websocket_;
  std::unique_ptr<binance_spot::MarketDataStream> stream_;
  std::thread thread_;
  std::atomic<bool> stopping_{false};
  absl::Status stream_error_;
};

}  // namespace hquant
