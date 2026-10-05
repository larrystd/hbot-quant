#include "order/order_tracker.h"

#include <utility>

#include "base/error.h"

namespace hquant {
namespace {

// 交易所状态的先后顺序：挂单 < 部分成交 < 结束。推送的消息如果比已知的
// 状态更早，说明是晚到的旧消息。
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
  // ---- 第一段：找到订单，过滤掉不可信的消息 ----
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
  // 推送的状态比已知的更早（例如已是部分成交，又晚到一条挂单），且不带
  // 成交：旧消息，忽略。带成交的不能丢，成交本身仍可能是新的。
  if (source == ReportSource::Push && entry.exchange_status &&
      Rank(update.status) < Rank(*entry.exchange_status) && !update.trade)
    return UpdateResult::Ignored;
  if (update.executed_quantity && !update.executed_quantity->IsNonnegative())
    return Error(ErrorCode::kOrderReportInvalid,
                 "negative cumulative execution");

  // 已经是一种结束状态，又推来另一种（例如已撤销，又说全部成交）。先记下，
  // 等成交处理完再处理：消息里的成交仍要累加，风控才能照常记账。
  const bool terminal_conflict =
      source == ReportSource::Push && entry.exchange_status &&
      Terminal(*entry.exchange_status) && Terminal(update.status) &&
      *entry.exchange_status != update.status;

  // ---- 第二段：处理成交（只在消息带成交时） ----
  bool changed = false;
  if (update.trade) {
    const Trade& trade = *update.trade;
    if (trade.exchange_trade_id.value.empty() ||
        !trade.quantity.IsStrictlyPositive() ||
        !trade.price.IsStrictlyPositive() || !trade.value.IsNonnegative())
      return Error(ErrorCode::kOrderReportInvalid, "invalid trade update");
    // 同一个成交编号已记在另一张单名下：数据矛盾。
    // 记在同一张单名下：重复推送，跳过成交，继续处理状态。
    const auto seen = seen_trades_.find(trade.exchange_trade_id.value);
    if (seen != seen_trades_.end() &&
        seen->second != update.client_order_id.value) {
      entry.status = Status::NeedsQuery;
      return Error(ErrorCode::kTradeIdOnOtherOrder,
                   "trade ID assigned to another order");
    }
    // 新成交：累加，且累计不能超过下单量。
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
  // ---- 第三段：记录交易所说的累计量和状态 ----
  // 结束状态冲突：标记需要查询。成交已累加时返回 Updated，让 Shard 照常记账；
  // 已经在等查询时不再重复报错。
  if (terminal_conflict) {
    const bool already_needs_query = entry.status == Status::NeedsQuery;
    entry.status = Status::NeedsQuery;
    if (changed) return UpdateResult::Updated;
    if (already_needs_query) return UpdateResult::Ignored;
    return Error(ErrorCode::kOrderStatusConflict,
                 "conflicting terminal order status");
  }
  // 记下交易所说的累计成交量。推送来的值比已记的小，是旧消息，不覆盖。
  if (update.executed_quantity) {
    if (entry.reported_quantity && source == ReportSource::Push &&
        entry.reported_quantity->Compare(*update.executed_quantity).value() >
            0) {
      // 旧的推送不能覆盖已记下的更新的累计量。
    } else if (!entry.reported_quantity ||
               entry.reported_quantity->Compare(*update.executed_quantity)
                       .value() != 0) {
      entry.reported_quantity = update.executed_quantity;
      changed = true;
    }
  }
  // 记下交易所说的状态。查询结果总是接受；推送只接受不倒退的。
  if (source == ReportSource::Query || !entry.exchange_status ||
      Rank(update.status) >= Rank(*entry.exchange_status)) {
    if (!entry.exchange_status || *entry.exchange_status != update.status) {
      entry.exchange_status = update.status;
      changed = true;
    }
  }
  if (!entry.exchange_status)
    return changed ? UpdateResult::Updated : UpdateResult::Ignored;
  // 一致性检查，任一不成立都标记需要查询：
  //   - 交易所说全部成交，它说的累计量必须等于下单量；
  //   - 我们累加的成交量不能超过交易所说的累计量。
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
  // ---- 第四段：推导结论状态 ----
  // 需要查询的订单，只有查询结果能让它恢复，推送到此为止。
  const Status before = entry.status;
  if (before == Status::NeedsQuery && source == ReportSource::Push)
    return changed ? UpdateResult::Updated : UpdateResult::Ignored;
  // 交易所说已结束：我们累加的成交量还没达到应有的量，说明成交明细没到齐，
  // 先进入 AwaitingTrades；达到了才真正结束。应有的量优先取交易所说的累计量；
  // 没有时，全部成交按下单量，其余按已累加的量（即不再等待）。
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
      // 正在撤单：普通的挂单或部分成交推送不改变它，要等撤单的结果。
    } else {
      entry.status = status == ExchangeOrderStatus::PartiallyTraded
                         ? Status::PartiallyTraded
                         : Status::Open;
    }
  }
  // 这次从未结束变成结束，返回 Finished，Shard 据此释放剩余的资金冻结。
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
