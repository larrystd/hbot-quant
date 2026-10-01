#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hbot/connector/api.h"
#include "hbot/connector/order_tracker.h"
#include "hbot/market_data/book_sync.h"
#include "hbot/model/market.h"
#include "hbot/model/trading_rule.h"
#include "hbot/runtime/dispatcher.h"

namespace hbot {

// G1 single-thread owner of strategy, book, Paper events, tracker and risk.
// The caller advances the clock and feeds inputs in InputStamp order.
class ShardRuntime {
 public:
  struct Config {
    RunId run;
    ShardId shard;
    OwnerId owner;
    AccountId account;
    MarketSpec market;
    BookScale scale;
    TradingRule rule;
    uint64_t stale_after_us = 60'000'000;
  };

  ShardRuntime(Config config, const Clock& clock, Strategy& strategy,
               SimulatedVenue& venue, RiskGate& risk, RecorderPort& recorder);

  BookApplyResult Subscribe(uint64_t stream_epoch);
  absl::Status OnSnapshot(const BookSnapshot& snapshot);
  absl::Status OnDiff(const BookDiff& diff);
  absl::Status OnPublicTrade(const PublicTrade& trade);
  // An external public stream may update this same BookSync and notify the
  // shard after each applied batch. All calls remain on the owning shard.
  BookSync& MutableBookSync() { return book_; }
  absl::Status OnBookApplied() { return RefreshPaperBbo(); }
  absl::StatusOr<std::vector<DispatchResult>> OnTimer(InputStamp stamp);

  const BookView& Book() const { return book_.View(); }
  std::optional<OrderSnapshot> Order(const ClientOrderId& id) const {
    return tracker_.Snapshot(id);
  }
  const std::vector<HistoryGap>& local_gaps() const { return local_gaps_; }
  uint64_t shard_sequence() const { return shard_sequence_; }

 private:
  absl::Status RefreshPaperBbo();
  absl::Status DrainPaperEvents();
  absl::Status ProcessAccountEvent(const AccountEvent& event);
  absl::Status Record(RecordPayload payload, const OwnerId& owner);
  void AddGap(uint64_t sequence);
  std::vector<OrderSnapshot> OrderViews() const;
  std::vector<Balance> BalanceViews() const;
  absl::StatusOr<Decimal> Price(PriceTicks ticks) const;
  absl::StatusOr<Decimal> Amount(QuantityLots lots) const;

  Config config_;
  const Clock& clock_;
  Strategy& strategy_;
  SimulatedVenue& venue_;
  RiskGate& risk_;
  RecorderPort& recorder_;
  BookSync book_;
  OrderTracker tracker_;
  uint64_t shard_sequence_ = 0;
  uint64_t next_decision_id_ = 1;
  ActionDispatcher dispatcher_;
  std::vector<ClientOrderId> order_ids_;
  std::map<std::string, ReservationId> reservations_;
  std::vector<HistoryGap> local_gaps_;
  std::optional<Decimal> last_trade_price_;
};

}  // namespace hbot
