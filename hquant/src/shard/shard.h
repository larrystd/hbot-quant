#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"
#include "base/market.h"
#include "base/order.h"
#include "base/types.h"
#include "market/order_book.h"
#include "order/order_tracker.h"
#include "shard/action_executor.h"
#include "storage/storage.h"
#include "strategy/strategy.h"

namespace hquant {

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
  StorageHealth storage_health;
};

// G1 single-thread owner of strategy, book, Simulated events, tracker and risk.
// The caller advances the clock and feeds inputs in InputTime order.
class Shard {
 public:
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
        SimulatedExchange& exchange, RiskGate& risk, HistoryWriter& recorder);

  BookApplyResult Subscribe(uint64_t connection_id);
  absl::Status OnSnapshot(const BookSnapshot& snapshot);
  absl::Status OnDiff(const BookDiff& diff);
  absl::Status OnPublicTrade(const PublicTrade& trade);
  // An external public stream may update this same OrderBookSync and notify the
  // shard after each applied batch. All calls remain on the owning shard.
  OrderBookSync& MutableBookSync() { return book_; }
  absl::Status OnBookApplied(const BookApplyResult& result);
  absl::StatusOr<std::vector<ActionResult>> OnTimer(InputTime stamp);

  const OrderBookView& Book() const { return book_.View(); }
  std::optional<OrderSnapshot> Order(const ClientOrderId& id) const {
    return tracker_.Snapshot(id);
  }
  const std::vector<HistoryGap>& local_gaps() const { return local_gaps_; }
  uint64_t shard_sequence() const { return shard_sequence_; }

 private:
  absl::Status UpdateSimulatedExchangeBbo();
  absl::Status AfterBookApply(const BookApplyResult& result);
  absl::Status RunPendingTrigger(std::optional<Trigger> explicit_trigger);
  absl::StatusOr<std::vector<ActionResult>> RunStrategy(Trigger why,
                                                       InputTime stamp,
                                                       bool allow_followup = true);
  absl::Status DrainSimulatedExchangeEvents();
  absl::Status ProcessAccountEvent(const AccountEvent& event);
  absl::Status Record(HistoryRecordPayload payload,
                      const StrategyId& strategy_id);
  void AddGap(uint64_t sequence);
  std::vector<OrderSnapshot> OrderViews() const;
  std::vector<Balance> BalanceViews() const;
  absl::StatusOr<Decimal> Price(PriceTicks ticks) const;
  absl::StatusOr<Decimal> Amount(QuantityLots lots) const;

  Config config_;
  const Clock& clock_;
  Strategy& strategy_;
  SimulatedExchange& exchange_;
  RiskGate& risk_;
  HistoryWriter& recorder_;
  OrderBookSync book_;
  OrderTracker tracker_;
  uint64_t shard_sequence_ = 0;
  uint64_t next_action_batch_id_ = 1;
  ActionExecutor action_executor_;
  std::vector<ClientOrderId> order_ids_;
  std::map<std::string, HoldId> holds_;
  std::vector<HistoryGap> local_gaps_;
  std::optional<Decimal> last_trade_price_;
  MonoTime origin_mono_{};
  uint64_t event_ordinal_ = 0;
  bool in_strategy_ = false;
  std::optional<Trigger> pending_trigger_;
  std::optional<MonoTime> last_strategy_at_;
};

}  // namespace hquant
