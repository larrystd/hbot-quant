#include "order/order_tracker.h"

#include <utility>

#include "base/error.h"

namespace hquant {
namespace {

int Rank(ExchangeOrderStatus status) {
  switch (status) {
    case ExchangeOrderStatus::Open:
      return 1;
    case ExchangeOrderStatus::PartiallyTraded:
      return 2;
    default:
      return 3;
  }
}

bool Terminal(ExchangeOrderStatus status) { return Rank(status) == 3; }

}  // namespace

bool OrderTracker::Finished(Status status) {
  return status == Status::Traded || status == Status::Canceled ||
         status == Status::Failed || status == Status::Expired;
}

bool OrderTracker::Active(Status status) {
  return status != Status::AwaitingTrades && !Finished(status);
}

OrderTracker::Status OrderTracker::TerminalStatus(ExchangeOrderStatus status) {
  switch (status) {
    case ExchangeOrderStatus::Traded:
      return Status::Traded;
    case ExchangeOrderStatus::Canceled:
      return Status::Canceled;
    case ExchangeOrderStatus::Rejected:
      return Status::Failed;
    case ExchangeOrderStatus::Expired:
      return Status::Expired;
    default:
      return Status::NeedsQuery;
  }
}

absl::Status OrderTracker::Add(const Order& order) {
  if (order.client_order_id.value.empty())
    return Error(ErrorCode::kClientOrderIdInvalid, "empty client order ID");
  if (!order.strategy_id.IsValid())
    return Error(ErrorCode::kOrderStrategyIdInvalid, "invalid strategy ID");
  if (order.account.value.empty())
    return Error(ErrorCode::kOrderAccountInvalid, "empty order account");
  if (order.market.exchange.value.empty() || order.market.native_symbol.empty())
    return Error(ErrorCode::kOrderMarketInvalid, "empty order market");
  if (!order.quantity.IsStrictlyPositive() || !order.price.IsStrictlyPositive())
    return Error(ErrorCode::kOrderPriceOrAmountInvalid,
                 "limit price and amount must be positive");
  if (!orders_.emplace(order.client_order_id.value, Entry{order}).second)
    return Error(ErrorCode::kOrderDuplicate, "client order ID already tracked");
  return absl::OkStatus();
}

absl::StatusOr<UpdateResult> OrderTracker::OnSendResult(const ClientOrderId& id,
                                                        SendResult result) {
  auto found = orders_.find(id.value);
  if (found == orders_.end())
    return Error(ErrorCode::kOrderNotFound, "order not tracked");
  Entry& entry = found->second;
  if (result == SendResult::Unknown) {
    if (entry.status == Status::SubmissionUnknown) return UpdateResult::Ignored;
    if (entry.status != Status::PendingCreate)
      return Error(ErrorCode::kOrderStatusConflict,
                   "submission already confirmed or terminal");
    entry.status = Status::SubmissionUnknown;
    return UpdateResult::Updated;
  }
  if (entry.status != Status::PendingCreate || entry.exchange_status ||
      entry.executed_quantity.IsStrictlyPositive())
    return Error(ErrorCode::kOrderNotCancelable,
                 "cannot prove no exchange write or fill");
  entry.status = Status::Failed;
  return UpdateResult::Finished;
}

absl::StatusOr<UpdateResult> OrderTracker::OnCancelRequested(
    const ClientOrderId& id) {
  auto found = orders_.find(id.value);
  if (found == orders_.end())
    return Error(ErrorCode::kOrderNotFound, "order not tracked");
  if (Finished(found->second.status) ||
      found->second.status == Status::AwaitingTrades ||
      found->second.status == Status::NeedsQuery)
    return Error(ErrorCode::kOrderNotCancelable, "order already terminal");
  if (found->second.status == Status::PendingCancel)
    return UpdateResult::Ignored;
  found->second.status = Status::PendingCancel;
  return UpdateResult::Updated;
}

absl::StatusOr<UpdateResult> OrderTracker::Apply(const OrderUpdate& update,
                                                 ReportSource source) {
  auto found = orders_.find(update.client_order_id.value);
  if (found == orders_.end())
    return Error(ErrorCode::kReportOrderUnknown, "order not tracked");
  Entry& entry = found->second;
  if (entry.order.account != update.account ||
      entry.order.market != update.market) {
    entry.status = Status::NeedsQuery;
    return Error(ErrorCode::kReportAccountMarketMismatch,
                 "account or market mismatch");
  }
  if (source == ReportSource::Push && entry.exchange_status &&
      Rank(update.status) < Rank(*entry.exchange_status) && !update.trade)
    return UpdateResult::Ignored;
  if (update.executed_quantity && !update.executed_quantity->IsNonnegative())
    return Error(ErrorCode::kOrderReportInvalid,
                 "negative cumulative execution");

  const bool terminal_conflict =
      source == ReportSource::Push && entry.exchange_status &&
      Terminal(*entry.exchange_status) && Terminal(update.status) &&
      *entry.exchange_status != update.status;

  bool changed = false;
  if (update.trade) {
    const Trade& trade = *update.trade;
    if (trade.exchange_trade_id.value.empty() ||
        !trade.quantity.IsStrictlyPositive() ||
        !trade.price.IsStrictlyPositive() || !trade.value.IsNonnegative())
      return Error(ErrorCode::kOrderReportInvalid, "invalid trade update");
    const auto seen = seen_trades_.find(trade.exchange_trade_id.value);
    if (seen != seen_trades_.end() &&
        seen->second != update.client_order_id.value) {
      entry.status = Status::NeedsQuery;
      return Error(ErrorCode::kTradeIdOnOtherOrder,
                   "trade ID assigned to another order");
    }
    if (seen == seen_trades_.end()) {
      auto total = entry.executed_quantity.Add(trade.quantity);
      if (!total.ok())
        return Error(ErrorCode::kOrderReportInvalid,
                     "trade cumulative base cannot be calculated");
      auto overfill = total->Compare(entry.order.quantity);
      if (!overfill.ok())
        return Error(ErrorCode::kOrderReportInvalid,
                     "trade fill amount cannot be compared");
      if (*overfill > 0) {
        entry.status = Status::NeedsQuery;
        return Error(ErrorCode::kTradeExceedsOrderAmount,
                     "trade exceeds requested base amount");
      }
      for (const auto& fee : trade.fees)
        if (fee.asset.value.empty())
          return Error(ErrorCode::kOrderReportInvalid, "fee asset missing");
      entry.executed_quantity = *total;
      seen_trades_.emplace(trade.exchange_trade_id.value,
                           update.client_order_id.value);
      changed = true;
    }
  }
  if (terminal_conflict) {
    const bool already_needs_query = entry.status == Status::NeedsQuery;
    entry.status = Status::NeedsQuery;
    if (changed) return UpdateResult::Updated;
    if (already_needs_query) return UpdateResult::Ignored;
    return Error(ErrorCode::kOrderStatusConflict,
                 "conflicting terminal order status");
  }
  if (update.executed_quantity) {
    if (entry.reported_quantity && source == ReportSource::Push &&
        entry.reported_quantity->Compare(*update.executed_quantity).value() >
            0) {
      // An older cumulative push cannot undo a verified newer report.
    } else if (!entry.reported_quantity ||
               entry.reported_quantity->Compare(*update.executed_quantity)
                       .value() != 0) {
      entry.reported_quantity = update.executed_quantity;
      changed = true;
    }
  }
  if (source == ReportSource::Query || !entry.exchange_status ||
      Rank(update.status) >= Rank(*entry.exchange_status)) {
    if (!entry.exchange_status || *entry.exchange_status != update.status) {
      entry.exchange_status = update.status;
      changed = true;
    }
  }
  if (!entry.exchange_status)
    return changed ? UpdateResult::Updated : UpdateResult::Ignored;
  const auto status = *entry.exchange_status;
  if (status == ExchangeOrderStatus::Traded && entry.reported_quantity &&
      entry.reported_quantity->Compare(entry.order.quantity).value() != 0) {
    entry.status = Status::NeedsQuery;
    if (changed) return UpdateResult::Updated;
    return Error(ErrorCode::kOrderStatusConflict,
                 "fully traded status has wrong cumulative quantity");
  }
  if (entry.reported_quantity &&
      entry.executed_quantity.Compare(*entry.reported_quantity).value() > 0) {
    entry.status = Status::NeedsQuery;
    if (changed) return UpdateResult::Updated;
    return Error(ErrorCode::kOrderStatusConflict,
                 "fill total exceeds reported cumulative quantity");
  }
  const Status before = entry.status;
  if (before == Status::NeedsQuery && source == ReportSource::Push)
    return changed ? UpdateResult::Updated : UpdateResult::Ignored;
  if (Terminal(status)) {
    const Decimal& expected =
        entry.reported_quantity
            ? *entry.reported_quantity
            : (status == ExchangeOrderStatus::Traded ? entry.order.quantity
                                                     : entry.executed_quantity);
    auto comparison = entry.executed_quantity.Compare(expected);
    if (!comparison.ok())
      return Error(ErrorCode::kDecimalArithmeticFailed,
                   "cannot compare execution totals");
    entry.status =
        *comparison < 0 ? Status::AwaitingTrades : TerminalStatus(status);
  } else if (entry.status != Status::NeedsQuery ||
             source == ReportSource::Query) {
    if (entry.status == Status::PendingCancel && source == ReportSource::Push) {
      // A normal order push does not resolve a pending cancel request.
    } else {
      entry.status = status == ExchangeOrderStatus::PartiallyTraded
                         ? Status::PartiallyTraded
                         : Status::Open;
    }
  }
  if (entry.status != before) changed = true;
  if (!Finished(before) && Finished(entry.status))
    return UpdateResult::Finished;
  return changed ? UpdateResult::Updated : UpdateResult::Ignored;
}

const Order* OrderTracker::Find(const ClientOrderId& id) const {
  auto found = orders_.find(id.value);
  return found == orders_.end() ? nullptr : &found->second.order;
}

bool OrderTracker::HasTrade(const ExchangeTradeId& id) const {
  return seen_trades_.contains(id.value);
}

std::vector<const Order*> OrderTracker::ActiveOrders() const {
  std::vector<const Order*> result;
  for (const auto& [_, entry] : orders_)
    if (Active(entry.status)) result.push_back(&entry.order);
  return result;
}

std::vector<ClientOrderId> OrderTracker::OrdersToQuery() const {
  std::vector<ClientOrderId> result;
  for (const auto& [id, entry] : orders_)
    if (entry.status == Status::SubmissionUnknown ||
        entry.status == Status::NeedsQuery ||
        entry.status == Status::AwaitingTrades)
      result.emplace_back(id);
  return result;
}

}  // namespace hquant
