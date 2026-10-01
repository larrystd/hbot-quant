#include "order/order_tracker.h"

#include <algorithm>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "base/error.h"

namespace hquant {
namespace {

int Rank(OrderLifecycle lifecycle) {
  switch (lifecycle) {
    case OrderLifecycle::PendingCreate:
      return 0;
    case OrderLifecycle::Open:
      return 1;
    case OrderLifecycle::PartiallyFilled:
      return 2;
    case OrderLifecycle::Filled:
    case OrderLifecycle::Canceled:
    case OrderLifecycle::Failed:
    case OrderLifecycle::Expired:
      return 3;
  }
  return 0;
}

OrderLifecycle Lifecycle(ExchangeOrderStatus status) {
  switch (status) {
    case ExchangeOrderStatus::New:
      return OrderLifecycle::Open;
    case ExchangeOrderStatus::PartiallyFilled:
      return OrderLifecycle::PartiallyFilled;
    case ExchangeOrderStatus::Filled:
      return OrderLifecycle::Filled;
    case ExchangeOrderStatus::Canceled:
      return OrderLifecycle::Canceled;
    case ExchangeOrderStatus::Rejected:
      return OrderLifecycle::Failed;
    case ExchangeOrderStatus::Expired:
      return OrderLifecycle::Expired;
  }
  return OrderLifecycle::Failed;
}

}  // namespace

OrderTracker::ExchangeKey OrderTracker::ExchangeIndexKey(
    const AccountId& account, const MarketId& market,
    const ExchangeOrderId& exchange_id) {
  return {account.value, market.venue.value,
          static_cast<int>(market.instrument_kind), market.native_symbol,
          exchange_id.value};
}

OrderTracker::TradeKey OrderTracker::TradeIndexKey(
    const AccountId& account, const MarketId& market,
    const ExchangeTradeId& trade_id) {
  return {account.value, market.venue.value,
          static_cast<int>(market.instrument_kind), market.native_symbol,
          trade_id.value};
}

bool OrderTracker::Terminal(OrderLifecycle lifecycle) {
  return Rank(lifecycle) == 3;
}

OrderDisplayState OrderTracker::Display(const TrackedOrder& order) const {
  if (order.lifecycle == OrderLifecycle::Filled &&
      order.completion_pending_fills)
    return OrderDisplayState::AwaitingFills;
  switch (order.lifecycle) {
    case OrderLifecycle::Filled:
      return OrderDisplayState::Filled;
    case OrderLifecycle::Canceled:
      return OrderDisplayState::Canceled;
    case OrderLifecycle::Failed:
      return OrderDisplayState::Failed;
    case OrderLifecycle::Expired:
      return OrderDisplayState::Expired;
    default:
      break;
  }
  if (order.reconciliation != ReconciliationState::Confirmed)
    return OrderDisplayState::SubmissionUnknown;
  if (order.cancel_pending) return OrderDisplayState::PendingCancel;
  switch (order.lifecycle) {
    case OrderLifecycle::PendingCreate:
      return OrderDisplayState::PendingCreate;
    case OrderLifecycle::Open:
      return OrderDisplayState::Open;
    case OrderLifecycle::PartiallyFilled:
      return OrderDisplayState::PartiallyFilled;
    default:
      return OrderDisplayState::Failed;
  }
}

OrderSnapshot OrderTracker::MakeSnapshot(const TrackedOrder& order) const {
  OrderSnapshot snapshot;
  snapshot.client_id = order.intent.client_id;
  snapshot.exchange_id = order.exchange_id;
  snapshot.owner = order.intent.owner;
  snapshot.request = order.intent.request;
  snapshot.display_state = Display(order);
  snapshot.cumulative_base = order.cumulative_base;
  snapshot.cumulative_quote = order.cumulative_quote;
  snapshot.last_update_time = order.last_update_time;
  for (const auto& [asset, amount] : order.fees_by_asset)
    snapshot.fees.push_back(TradeFee{AssetId{asset}, amount});
  return snapshot;
}

TrackerResult OrderTracker::MakeResult(
    const TrackedOrder& order, bool changed,
    std::vector<TrackedOrderEvent> events) const {
  TrackerResult result;
  result.snapshot = MakeSnapshot(order);
  result.events = std::move(events);
  result.changed = changed;
  result.cancel_pending = order.cancel_pending;
  result.reconciliation = order.reconciliation;
  result.needs_reconciliation =
      order.completion_pending_fills ||
      order.reconciliation != ReconciliationState::Confirmed;
  return result;
}

absl::StatusOr<TrackerResult> OrderTracker::Register(OrderIntent intent) {
  if (intent.client_id.value.empty() || !intent.owner.IsValid() ||
      intent.request.account.value.empty() ||
      intent.request.market.venue.value.empty() ||
      intent.request.market.native_symbol.empty() ||
      !intent.request.base_amount.IsStrictlyPositive() ||
      !intent.request.limit_price ||
      !intent.request.limit_price->IsStrictlyPositive())
    return Error(ErrorCode::kOrderInvalid, "invalid order intent");
  if (orders_.contains(intent.client_id.value))
    return Error(ErrorCode::kOrderDuplicate, "client order ID already tracked");
  TrackedOrder order;
  order.last_update_time.receive_utc = intent.created_at_utc;
  order.intent = std::move(intent);
  auto [it, inserted] =
      orders_.emplace(order.intent.client_id.value, std::move(order));
  (void)inserted;
  return MakeResult(it->second, true);
}

absl::StatusOr<OrderTracker::TrackedOrder*> OrderTracker::Find(
    const AccountId& account, const MarketId& market,
    const std::optional<ClientOrderId>& client_id,
    const std::optional<ExchangeOrderId>& exchange_id) {
  if ((!client_id || client_id->value.empty()) &&
      (!exchange_id || exchange_id->value.empty()))
    return Error(ErrorCode::kOrderReportInvalid, "report has no order ID");
  TrackedOrder* by_client = nullptr;
  TrackedOrder* by_exchange = nullptr;
  if (client_id) {
    auto found = orders_.find(client_id->value);
    if (found != orders_.end()) by_client = &found->second;
  }
  if (exchange_id) {
    auto found =
        exchange_index_.find(ExchangeIndexKey(account, market, *exchange_id));
    if (found != exchange_index_.end()) {
      auto order = orders_.find(found->second);
      if (order != orders_.end()) by_exchange = &order->second;
    }
  }
  if (by_client && by_exchange && by_client != by_exchange) {
    by_client->reconciliation = ReconciliationState::ResyncRequired;
    by_exchange->reconciliation = ReconciliationState::ResyncRequired;
    return Error(ErrorCode::kOrderReportConflict,
                 "client and exchange IDs identify different orders");
  }
  if (client_id && !by_client && by_exchange) {
    by_exchange->reconciliation = ReconciliationState::ResyncRequired;
    return Error(ErrorCode::kOrderReportConflict,
                 "unknown client ID conflicts with exchange ID");
  }
  TrackedOrder* order = by_client ? by_client : by_exchange;
  if (!order)
    return Error(ErrorCode::kOrderReportUnattributed,
                 "order report has no tracked owner");
  if (order->intent.request.account != account ||
      order->intent.request.market != market) {
    order->reconciliation = ReconciliationState::ResyncRequired;
    return Error(ErrorCode::kOrderReportConflict, "account or market mismatch");
  }
  if (order->exchange_id && exchange_id &&
      *order->exchange_id != *exchange_id) {
    order->reconciliation = ReconciliationState::ResyncRequired;
    return Error(ErrorCode::kOrderReportConflict,
                 "exchange ID changed for tracked client ID");
  }
  return order;
}

absl::Status OrderTracker::BindExchangeId(
    TrackedOrder& order, const std::optional<ExchangeOrderId>& exchange_id) {
  if (!exchange_id) return absl::OkStatus();
  if (order.exchange_id) {
    if (*order.exchange_id != *exchange_id) {
      order.reconciliation = ReconciliationState::ResyncRequired;
      return Error(ErrorCode::kOrderReportConflict, "exchange ID changed");
    }
    return absl::OkStatus();
  }
  const auto key = ExchangeIndexKey(order.intent.request.account,
                                    order.intent.request.market, *exchange_id);
  if (auto found = exchange_index_.find(key);
      found != exchange_index_.end() &&
      found->second != order.intent.client_id.value) {
    order.reconciliation = ReconciliationState::ResyncRequired;
    return Error(ErrorCode::kOrderReportConflict,
                 "exchange ID already owned by another order");
  }
  order.exchange_id = *exchange_id;
  exchange_index_[key] = order.intent.client_id.value;
  return absl::OkStatus();
}

absl::StatusOr<TrackerResult> OrderTracker::RequestCancel(
    const ClientOrderId& client_id) {
  auto found = orders_.find(client_id.value);
  if (found == orders_.end())
    return Error(ErrorCode::kOrderNotFound, "order not tracked");
  auto& order = found->second;
  if (Terminal(order.lifecycle))
    return Error(ErrorCode::kOrderNotCancelable, "order already terminal");
  const bool changed = !order.cancel_pending;
  order.cancel_pending = true;
  return MakeResult(order, changed);
}

absl::StatusOr<TrackerResult> OrderTracker::FailBeforeWrite(
    const ClientOrderId& client_id, std::string reason) {
  auto found = orders_.find(client_id.value);
  if (found == orders_.end())
    return Error(ErrorCode::kOrderNotFound, "order not tracked");
  auto& order = found->second;
  auto compared = order.cumulative_base.Compare(Decimal{});
  if (!compared.ok())
    return Error(ErrorCode::kInternal,
                 "tracked cumulative amount cannot be compared");
  if (order.lifecycle != OrderLifecycle::PendingCreate ||
      order.reconciliation != ReconciliationState::Confirmed ||
      order.exchange_id || order.created_emitted || *compared != 0)
    return Error(ErrorCode::kOrderNotCancelable,
                 "cannot prove no exchange write or fill");
  if (reason.empty()) reason = "LocalFailure";
  order.lifecycle = OrderLifecycle::Failed;
  order.terminal_emitted = true;
  std::vector<TrackedOrderEvent> events;
  events.emplace_back(OrderFailed{MakeSnapshot(order), std::move(reason)});
  return MakeResult(order, true, std::move(events));
}

absl::StatusOr<TrackerResult> OrderTracker::MarkSubmissionUnknown(
    const ClientOrderId& client_id) {
  auto found = orders_.find(client_id.value);
  if (found == orders_.end())
    return Error(ErrorCode::kOrderNotFound, "order not tracked");
  auto& order = found->second;
  if (order.lifecycle != OrderLifecycle::PendingCreate)
    return Error(ErrorCode::kOrderReportConflict,
                 "submission already confirmed or terminal");
  const bool changed =
      order.reconciliation != ReconciliationState::SubmissionUnknown;
  order.reconciliation = ReconciliationState::SubmissionUnknown;
  return MakeResult(order, changed);
}

absl::StatusOr<TrackerResult> OrderTracker::ApplyOrderUpdate(
    const OrderUpdate& update) {
  return Update(update, false);
}

absl::StatusOr<TrackerResult> OrderTracker::Reconcile(
    const OrderUpdate& update) {
  return Update(update, true);
}

absl::StatusOr<TrackerResult> OrderTracker::Update(const OrderUpdate& update,
                                                   bool reconciled) {
  auto found = Find(update.account, update.market, update.client_id,
                    update.exchange_order_id);
  if (!found.ok()) return found.status();
  TrackedOrder& order = **found;
  const auto previous_exchange = order.exchange_id;
  auto bind = BindExchangeId(order, update.exchange_order_id);
  if (!bind.ok()) return bind;
  bool changed = previous_exchange != order.exchange_id;
  const OrderLifecycle next = Lifecycle(update.exchange_status);
  if (Terminal(order.lifecycle) && next != order.lifecycle) {
    order.reconciliation = ReconciliationState::ResyncRequired;
    return Error(ErrorCode::kOrderReportConflict,
                 "conflicting terminal order status");
  }
  if (Rank(next) < Rank(order.lifecycle)) {
    return MakeResult(order, changed);  // stale HTTP/status result
  }
  std::vector<TrackedOrderEvent> events;
  bool created_now = false;
  if (order.lifecycle == OrderLifecycle::PendingCreate &&
      next != OrderLifecycle::Canceled && next != OrderLifecycle::Failed &&
      next != OrderLifecycle::Expired && !order.created_emitted) {
    order.created_emitted = true;
    created_now = true;
    changed = true;
  }
  if (order.lifecycle != next) {
    order.lifecycle = next;
    changed = true;
  }
  if (order.reconciliation == ReconciliationState::SubmissionUnknown ||
      (reconciled &&
       order.reconciliation == ReconciliationState::ResyncRequired)) {
    order.reconciliation = ReconciliationState::Confirmed;
    changed = true;
  }
  if (update.cumulative_base) {
    order.reported_cumulative_base = update.cumulative_base;
    changed = true;
  }
  if (update.cumulative_quote) {
    order.reported_cumulative_quote = update.cumulative_quote;
    changed = true;
  }
  if (Terminal(order.lifecycle) && order.cancel_pending) {
    order.cancel_pending = false;
    changed = true;
  }
  if (order.lifecycle == OrderLifecycle::Filled) {
    auto comparison =
        order.cumulative_base.Compare(order.intent.request.base_amount);
    if (!comparison.ok())
      return Error(ErrorCode::kOrderReportConflict,
                   "reported cumulative amount cannot be compared");
    const bool awaiting = *comparison < 0;
    if (order.completion_pending_fills != awaiting) changed = true;
    order.completion_pending_fills = awaiting;
  }
  if (changed) order.last_update_time = update.time;
  if (created_now) events.emplace_back(OrderCreated{MakeSnapshot(order)});
  if (next == OrderLifecycle::Filled && !order.completion_pending_fills &&
      !order.terminal_emitted) {
    order.terminal_emitted = true;
    events.emplace_back(OrderCompleted{MakeSnapshot(order)});
  } else if (next == OrderLifecycle::Canceled && !order.terminal_emitted) {
    order.terminal_emitted = true;
    events.emplace_back(OrderCanceled{MakeSnapshot(order)});
  } else if ((next == OrderLifecycle::Failed ||
              next == OrderLifecycle::Expired) &&
             !order.terminal_emitted) {
    order.terminal_emitted = true;
    events.emplace_back(
        OrderFailed{MakeSnapshot(order),
                    next == OrderLifecycle::Expired ? "Expired" : "Rejected"});
  }
  return MakeResult(order, changed, std::move(events));
}

absl::StatusOr<TrackerResult> OrderTracker::ApplyTradeUpdate(
    const TradeUpdate& trade) {
  auto found = Find(trade.account, trade.market, trade.client_id,
                    trade.exchange_order_id);
  if (!found.ok()) return found.status();
  TrackedOrder& order = **found;
  const auto previous_exchange = order.exchange_id;
  auto bind = BindExchangeId(order, trade.exchange_order_id);
  if (!bind.ok()) return bind;
  const bool bound = previous_exchange != order.exchange_id;
  if (trade.exchange_trade_id.value.empty() ||
      !trade.base_amount.IsStrictlyPositive() ||
      !trade.price.IsStrictlyPositive() || !trade.quote_amount.IsNonnegative())
    return Error(ErrorCode::kOrderReportInvalid, "invalid trade update");
  const auto key =
      TradeIndexKey(trade.account, trade.market, trade.exchange_trade_id);
  if (auto seen = seen_trades_.find(key); seen != seen_trades_.end()) {
    if (seen->second != order.intent.client_id.value) {
      order.reconciliation = ReconciliationState::ResyncRequired;
      return Error(ErrorCode::kOrderReportConflict,
                   "trade ID assigned to another order");
    }
    return MakeResult(order, bound);
  }
  auto base = order.cumulative_base.Add(trade.base_amount);
  auto quote = order.cumulative_quote.Add(trade.quote_amount);
  if (!base.ok())
    return Error(ErrorCode::kOrderReportInvalid,
                 "trade cumulative base cannot be calculated");
  if (!quote.ok())
    return Error(ErrorCode::kOrderReportInvalid,
                 "trade cumulative quote cannot be calculated");
  auto overfill = base->Compare(order.intent.request.base_amount);
  if (!overfill.ok())
    return Error(ErrorCode::kOrderReportInvalid,
                 "trade fill amount cannot be compared");
  if (*overfill > 0) {
    order.reconciliation = ReconciliationState::ResyncRequired;
    return Error(ErrorCode::kOrderReportConflict,
                 "trade exceeds requested base amount");
  }
  auto fees = order.fees_by_asset;
  for (const auto& fee : trade.fees) {
    if (fee.asset.value.empty())
      return Error(ErrorCode::kOrderReportInvalid, "fee asset missing");
    auto sum = fees[fee.asset.value].Add(fee.signed_amount);
    if (!sum.ok())
      return Error(ErrorCode::kOrderReportInvalid,
                   "trade fee total cannot be calculated");
    fees[fee.asset.value] = *sum;
  }
  order.cumulative_base = *base;
  order.cumulative_quote = *quote;
  order.fees_by_asset = std::move(fees);
  order.last_update_time = trade.time;
  seen_trades_[key] = order.intent.client_id.value;
  std::vector<TrackedOrderEvent> events;
  events.emplace_back(OrderFilled{MakeSnapshot(order), trade});
  if (order.lifecycle == OrderLifecycle::Filled &&
      order.completion_pending_fills && *overfill == 0 &&
      !order.terminal_emitted) {
    order.completion_pending_fills = false;
    order.terminal_emitted = true;
    events.emplace_back(OrderCompleted{MakeSnapshot(order)});
  }
  return MakeResult(order, true, std::move(events));
}

std::optional<OrderSnapshot> OrderTracker::Snapshot(
    const ClientOrderId& client_id) const {
  auto found = orders_.find(client_id.value);
  if (found == orders_.end()) return std::nullopt;
  return MakeSnapshot(found->second);
}

std::vector<ClientOrderId> OrderTracker::ReconciliationQueue() const {
  std::vector<ClientOrderId> ids;
  for (const auto& [client_id, order] : orders_) {
    if (order.completion_pending_fills ||
        order.reconciliation != ReconciliationState::Confirmed)
      ids.emplace_back(client_id);
  }
  return ids;
}

}  // namespace hquant
