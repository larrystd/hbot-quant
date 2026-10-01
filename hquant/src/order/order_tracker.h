#pragma once

#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/order.h"

namespace hquant {

enum class OrderLifecycle {
  PendingCreate,
  Open,
  PartiallyTraded,
  Traded,
  Canceled,
  Failed,
  Expired
};
enum class ConfirmationState { Confirmed, SubmissionUnknown, NeedsQuery };

struct TrackerResult {
  OrderSnapshot snapshot;
  std::vector<TrackedOrderEvent> events;
  bool changed = false;
  bool cancel_pending = false;
  bool needs_order_query = false;
  ConfirmationState confirmation = ConfirmationState::Confirmed;
};

// Single-shard, synchronous state machine. It never calls a strategy or risk
// gate; the owning runtime updates risk from the returned snapshot before
// dispatching the ordered events.
class OrderTracker {
 public:
  absl::StatusOr<TrackerResult> Register(PreparedOrder prepared);
  absl::StatusOr<TrackerResult> RequestCancel(const ClientOrderId& client_id);
  // A failure proven to occur before any socket write releases PendingCreate.
  absl::StatusOr<TrackerResult> FailBeforeWrite(const ClientOrderId& client_id,
                                                std::string reason);
  absl::StatusOr<TrackerResult> MarkSubmissionUnknown(
      const ClientOrderId& client_id);
  absl::StatusOr<TrackerResult> ApplyOrderUpdate(const OrderUpdate& update);
  absl::StatusOr<TrackerResult> ApplyTradeUpdate(const TradeUpdate& trade);
  // REST/private confirmation confirms the original client ID. No resend.
  absl::StatusOr<TrackerResult> ApplyQueriedOrder(const OrderUpdate& update);

  std::optional<OrderSnapshot> Snapshot(const ClientOrderId& client_id) const;
  std::vector<ClientOrderId> OrdersNeedingQuery() const;

 private:
  struct TrackedOrder {
    PreparedOrder prepared;
    std::optional<ExchangeOrderId> exchange_id;
    OrderLifecycle lifecycle = OrderLifecycle::PendingCreate;
    bool cancel_pending = false;
    ConfirmationState confirmation = ConfirmationState::Confirmed;
    bool completion_pending_fills = false;
    bool created_emitted = false;
    bool terminal_emitted = false;
    Decimal cumulative_base;
    Decimal cumulative_quote;
    std::map<std::string, Decimal> fees_by_asset;
    std::optional<Decimal> reported_cumulative_base;
    std::optional<Decimal> reported_cumulative_quote;
    EventTime last_update_time;
  };
  using ExchangeKey =
      std::tuple<std::string, std::string, int, std::string, std::string>;
  using TradeKey =
      std::tuple<std::string, std::string, int, std::string, std::string>;

  static ExchangeKey ExchangeIndexKey(const AccountId& account,
                                      const MarketId& market,
                                      const ExchangeOrderId& exchange_id);
  static TradeKey TradeIndexKey(const AccountId& account,
                                const MarketId& market,
                                const ExchangeTradeId& trade_id);
  static bool Terminal(OrderLifecycle lifecycle);
  OrderDisplayState Display(const TrackedOrder& order) const;
  OrderSnapshot MakeSnapshot(const TrackedOrder& order) const;
  TrackerResult MakeResult(const TrackedOrder& order, bool changed,
                           std::vector<TrackedOrderEvent> events = {}) const;
  absl::StatusOr<TrackedOrder*> Find(
      const AccountId& account, const MarketId& market,
      const std::optional<ClientOrderId>& client_id,
      const std::optional<ExchangeOrderId>& exchange_id);
  absl::Status BindExchangeId(
      TrackedOrder& order, const std::optional<ExchangeOrderId>& exchange_id);
  absl::StatusOr<TrackerResult> Update(const OrderUpdate& update,
                                       bool queried_order);

  std::map<std::string, TrackedOrder> orders_;
  std::map<ExchangeKey, std::string> exchange_index_;
  std::map<TradeKey, std::string> seen_trades_;
};

}  // namespace hquant
