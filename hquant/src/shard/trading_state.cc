#include "hquant/shard/trading_state.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <utility>

#include "hquant/base/fixed.h"
#include "hquant/trading/risk_limits.h"

namespace hquant::v1 {
namespace {
constexpr uint64_t kLocalIdBits = 40;
constexpr uint64_t kLocalIdMask = (uint64_t{1} << kLocalIdBits) - 1;
}

ShardTradingState::ShardTradingState(
    const ShardConfig& shard, ShardAllocation allocation,
    AccountConfig::RiskPolicy risk, uint64_t maker_fee_rate_ppb)
    : shard_(shard),
      risk_(risk),
      maker_fee_rate_ppb_(maker_fee_rate_ppb),
      balances_(std::move(allocation.assets_nanos)),
      max_active_orders_(allocation.max_active_orders),
      max_open_value_per_quote_nanos_(
          allocation.max_open_value_per_quote_nanos) {
  markets_.emplace(shard_.id.value, shard_.market);
  if (max_active_orders_) risk_.max_active_orders = *max_active_orders_;
  if (max_open_value_per_quote_nanos_)
    risk_.max_open_value_per_quote_nanos =
        *max_open_value_per_quote_nanos_;
}

uint64_t ShardTradingState::Total(const std::string& asset) const {
  const auto found = balances_.find(asset);
  return found == balances_.end() ? 0 : found->second;
}

uint64_t ShardTradingState::Available(const std::string& asset) const {
  const uint64_t total = Total(asset);
  const auto found = reserved_.find(asset);
  const uint64_t reserved = found == reserved_.end() ? 0 : found->second;
  return reserved >= total ? 0 : total - reserved;
}

absl::StatusOr<uint64_t> ShardTradingState::ReserveAmount(
    const LimitOrderRequest& request) const {
  if (request.side == Side::Sell) return request.quantity_nanos;
  auto gross =
      MulDivCeil(request.price_nanos, request.quantity_nanos, kFixedScale);
  if (!gross.ok()) return gross.status();
  auto fee = MulDivCeil(*gross, maker_fee_rate_ppb_, kFixedScale);
  if (!fee.ok()) return fee.status();
  auto amount = CheckedAdd(*gross, *fee);
  if (!amount.ok()) return amount.status();
  if (maker_fee_rate_ppb_ == 0) return *amount;
  return CheckedAdd(*amount, request.quantity_nanos /
                                 shard_.market.rule.base_increment_nanos);
}

absl::Status ShardTradingState::Reserve(const ClientOrderId& id,
                                        std::string asset, uint64_t amount) {
  if (amount == 0 || asset.empty() || holds_.contains(id.value))
    return absl::InvalidArgumentError("invalid shard reservation");
  if (Available(asset) < amount)
    return absl::ResourceExhaustedError("shard asset allocation exhausted");
  reserved_[asset] += amount;
  holds_.emplace(id.value, Hold{std::move(asset), amount});
  return absl::OkStatus();
}

absl::Status ShardTradingState::SetHold(const ClientOrderId& id,
                                        uint64_t amount) {
  const auto found = holds_.find(id.value);
  if (found == holds_.end() || amount > found->second.amount_nanos)
    return absl::DataLossError("invalid shard hold adjustment");
  reserved_[found->second.asset] -= found->second.amount_nanos - amount;
  found->second.amount_nanos = amount;
  return absl::OkStatus();
}

absl::Status ShardTradingState::Release(const ClientOrderId& id) {
  const auto found = holds_.find(id.value);
  if (found == holds_.end())
    return absl::NotFoundError("shard reservation missing");
  reserved_[found->second.asset] -= found->second.amount_nanos;
  holds_.erase(found);
  return absl::OkStatus();
}

absl::StatusOr<ShardTradingState::Admission> ShardTradingState::BeginSubmit(
    ClientOrderId id, const LimitOrderRequest& request,
    uint64_t source_bench_seq) {
  if ((max_active_orders_ && *max_active_orders_ == 0) ||
      (max_open_value_per_quote_nanos_ &&
       *max_open_value_per_quote_nanos_ == 0)) {
    return absl::ResourceExhaustedError("shard risk allocation exhausted");
  }
  if (auto status = RiskLimits::Check(
          risk_, shard_.market, request, markets_, orders_.State(),
          venue_rule_ ? &*venue_rule_ : nullptr);
      !status.ok())
    return status;
  auto amount = ReserveAmount(request);
  if (!amount.ok()) return amount.status();
  if (next_local_sequence_ > kLocalIdMask)
    return absl::OutOfRangeError("shard local order id exhausted");
  const std::string asset = request.side == Side::Buy
                                ? shard_.market.quote_asset
                                : shard_.market.base_asset;
  if (auto status = Reserve(id, asset, *amount); !status.ok()) return status;
  TrackedOrder order;
  order.client_order_id = id;
  order.local_order_id =
      OrderId{((uint64_t{shard_.id.value} + 1) << kLocalIdBits) |
              next_local_sequence_};
  order.shard = shard_.id;
  order.market = shard_.market.id;
  order.side = request.side;
  order.price_nanos = request.price_nanos;
  order.quantity_nanos = request.quantity_nanos;
  order.source_bench_seq = source_bench_seq;
  if (auto status = orders_.Begin(order); !status.ok()) {
    (void)Release(id);
    return status;
  }
  ++next_local_sequence_;
  return Admission{std::move(order), asset, *amount};
}

absl::Status ShardTradingState::AbortSubmit(const ClientOrderId& id) {
  if (auto status = orders_.AbortSubmit(id); !status.ok()) return status;
  return Release(id);
}

absl::StatusOr<CancelCommand> ShardTradingState::BeginCancel(
    OrderId local_id, uint64_t source_bench_seq) {
  const TrackedOrder* order = orders_.FindLocal(local_id);
  if (!order) return absl::NotFoundError("shard order not found");
  const ClientOrderId id = order->client_order_id;
  const CancelCommand command{order->market, id, order->exchange_order_id};
  const uint64_t requested_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
  if (auto status = orders_.BeginCancel(id, source_bench_seq, requested_ms);
      !status.ok())
    return status;
  return command;
}

absl::Status ShardTradingState::AbortCancel(const ClientOrderId& id) {
  return orders_.AbortCancel(id);
}

absl::Status ShardTradingState::MarkWriting(const ClientOrderId& id,
                                            bool cancel) {
  return orders_.MarkWriting(id, cancel);
}

Order ShardTradingState::ReportOrder(const TrackedOrder& tracked) {
  Order order;
  order.id = tracked.local_order_id;
  order.side = tracked.side;
  order.price_nanos = tracked.price_nanos;
  order.quantity_nanos = tracked.quantity_nanos;
  order.remaining_nanos = tracked.Remaining();
  order.market = tracked.market;
  order.client_order_id = tracked.client_order_id;
  order.exchange_order_id = tracked.exchange_order_id;
  return order;
}

absl::Status ShardTradingState::ApplyReport(const OrderReport& report) {
  auto applied = orders_.ApplyReport(report);
  if (!applied.ok()) return applied.status();
  const TrackedOrder* order = orders_.Find(report.order.client_order_id);
  if (order && order->Terminal() &&
      holds_.contains(report.order.client_order_id.value)) {
    return Release(report.order.client_order_id);
  }
  return absl::OkStatus();
}

absl::Status ShardTradingState::ApplyFill(const Fill& fill) {
  const TrackedOrder* order = orders_.Find(fill.client_order_id);
  if (!order) return absl::NotFoundError("fill for unknown shard order");
  if (order->trade_ids.contains(fill.trade_id)) return absl::OkStatus();
  std::map<std::string, uint64_t> next = balances_;
  const auto debit = [&next](const std::string& asset,
                             uint64_t amount) -> absl::Status {
    const auto found = next.find(asset);
    if (found == next.end() || found->second < amount)
      return absl::DataLossError("fill exceeds shard asset allocation: " +
                                 asset);
    found->second -= amount;
    return absl::OkStatus();
  };
  const auto credit = [&next](const std::string& asset,
                              uint64_t amount) -> absl::Status {
    const auto found = next.find(asset);
    if (found == next.end() ||
        amount > std::numeric_limits<uint64_t>::max() - found->second)
      return absl::DataLossError("fill asset allocation overflow: " + asset);
    found->second += amount;
    return absl::OkStatus();
  };
  if (fill.side == Side::Buy) {
    if (auto status = debit(shard_.market.quote_asset, fill.gross_nanos);
        !status.ok())
      return status;
    if (auto status = credit(shard_.market.base_asset, fill.quantity_nanos);
        !status.ok())
      return status;
  } else {
    if (auto status = debit(shard_.market.base_asset, fill.quantity_nanos);
        !status.ok())
      return status;
    if (auto status = credit(shard_.market.quote_asset, fill.gross_nanos);
        !status.ok())
      return status;
  }
  if (fill.fee_nanos != 0) {
    if (auto status = debit(fill.fee_asset, fill.fee_nanos); !status.ok())
      return status;
  }
  auto applied = orders_.ApplyFill(fill);
  if (!applied.ok()) return applied.status();
  if (!*applied) return absl::OkStatus();
  balances_ = std::move(next);
  order = orders_.Find(fill.client_order_id);
  if (!holds_.contains(fill.client_order_id.value)) return absl::OkStatus();
  if (order->Remaining() == 0) return Release(fill.client_order_id);
  auto remaining = ReserveAmount(LimitOrderRequest{
      order->side, order->price_nanos, order->Remaining()});
  if (!remaining.ok()) return remaining.status();
  return SetHold(fill.client_order_id, *remaining);
}

absl::Status ShardTradingState::ApplyEvent(const AccountEvent& event) {
  if (const auto* report = std::get_if<OrderReport>(&event))
    return ApplyReport(*report);
  if (const auto* fill = std::get_if<Fill>(&event)) return ApplyFill(*fill);
  return absl::OkStatus();
}

absl::Status ShardTradingState::ApplyResponse(
    const ClientOrderId& id, bool cancel, const TradeResponse& response) {
  const TrackedOrder* order = orders_.Find(id);
  if (!order) return absl::NotFoundError("response for unknown shard order");
  if (cancel) {
    if (order->Terminal()) return absl::OkStatus();
    if (response.result == TradeResponse::Result::NotSent) {
      if (order->cancel_writing)
        return absl::DataLossError("unsent shard cancel was written");
      return orders_.AbortCancel(id);
    }
    if (response.result == TradeResponse::Result::Unknown)
      return orders_.MarkCancelUnknown(id);
    if (response.result == TradeResponse::Result::Rejected)
      return orders_.MarkCancelRejected(id);
    if (response.remaining_nanos &&
        *response.remaining_nanos < order->Remaining())
      return absl::OkStatus();
    Order reported = ReportOrder(*order);
    if (response.remaining_nanos)
      reported.remaining_nanos = *response.remaining_nanos;
    return ApplyReport(OrderReport{std::move(reported),
                                   OrderStatus::Cancelled});
  }
  if (response.result == TradeResponse::Result::NotSent) {
    if (order->status != OrderStatus::PendingSubmit || order->submit_writing)
      return absl::DataLossError("unsent shard submit was written");
    return AbortSubmit(id);
  }
  if (response.result == TradeResponse::Result::Unknown) {
    if (order->status != OrderStatus::PendingSubmit) return absl::OkStatus();
    return orders_.MarkSubmitUnknown(id);
  }
  if (response.result == TradeResponse::Result::Rejected) {
    if (order->status == OrderStatus::Rejected) return absl::OkStatus();
    if (order->status != OrderStatus::PendingSubmit)
      return absl::DataLossError("shard submit rejection conflicts with report");
    return ApplyReport(OrderReport{ReportOrder(*order),
                                   OrderStatus::Rejected, response.reason});
  }
  if (order->status == OrderStatus::PendingSubmit) {
    Order reported = ReportOrder(*order);
    reported.exchange_order_id = response.exchange_order_id;
    return ApplyReport(
        OrderReport{std::move(reported), OrderStatus::Accepted});
  }
  return absl::OkStatus();
}

absl::Status ShardTradingState::BeginRestored(const JournalEntry& entry) {
  if (entry.shard != shard_.id.value || entry.symbol != shard_.market.id.symbol ||
      entry.side > 1 || entry.local_id == 0 || entry.amount == 0)
    return absl::DataLossError("invalid shard submit intent");
  const ClientOrderId id{entry.client_id};
  if (auto status = Reserve(id, entry.asset, entry.amount); !status.ok())
    return status;
  TrackedOrder order;
  order.client_order_id = id;
  order.local_order_id = OrderId{entry.local_id};
  order.shard = shard_.id;
  order.market = shard_.market.id;
  order.side = entry.side == 0 ? Side::Buy : Side::Sell;
  order.price_nanos = entry.price;
  order.quantity_nanos = entry.quantity;
  order.source_bench_seq = entry.source_bench_seq;
  if (auto status = orders_.Begin(std::move(order)); !status.ok()) {
    (void)Release(id);
    return status;
  }
  const uint64_t prefix =
      (uint64_t{shard_.id.value} + 1) << kLocalIdBits;
  if ((entry.local_id & ~kLocalIdMask) == prefix) {
    next_local_sequence_ =
        std::max(next_local_sequence_, (entry.local_id & kLocalIdMask) + 1);
  }
  return absl::OkStatus();
}

absl::Status ShardTradingState::RestoreJournal(
    const std::vector<JournalEntry>& entries) {
  for (const JournalEntry& entry : entries) {
    const ClientOrderId id{entry.client_id};
    if (entry.kind == JournalKind::SubmitIntent) {
      if (entry.shard == shard_.id.value) {
        if (auto status = BeginRestored(entry); !status.ok()) return status;
      }
      continue;
    }
    if (!orders_.Find(id)) continue;
    absl::Status status;
    switch (entry.kind) {
      case JournalKind::SubmitAbort:
        status = AbortSubmit(id);
        break;
      case JournalKind::CancelIntent:
        status = orders_.BeginCancel(id, entry.source_bench_seq,
                                     entry.transaction_ms);
        break;
      case JournalKind::CancelAbort:
        status = AbortCancel(id);
        break;
      case JournalKind::SubmitWriting:
      case JournalKind::CancelWriting:
        status = MarkWriting(id, entry.kind == JournalKind::CancelWriting);
        break;
      case JournalKind::SubmitOutcome:
      case JournalKind::CancelOutcome: {
        if (entry.result >
            static_cast<uint64_t>(TradeResponse::Result::NotSent))
          return absl::DataLossError("invalid shard trade response");
        TradeResponse response{
            static_cast<TradeResponse::Result>(entry.result),
            ExchangeOrderId{entry.exchange_id}, entry.message};
        if (entry.kind == JournalKind::CancelOutcome && entry.credit)
          response.remaining_nanos = entry.remaining;
        status = ApplyResponse(id, entry.kind == JournalKind::CancelOutcome,
                               response);
        break;
      }
      case JournalKind::OrderReport: {
        Order order;
        order.market = MarketId{entry.symbol};
        order.client_order_id = id;
        order.exchange_order_id = ExchangeOrderId{entry.exchange_id};
        order.side = entry.side == 0 ? Side::Buy : Side::Sell;
        order.price_nanos = entry.price;
        order.quantity_nanos = entry.quantity;
        order.remaining_nanos = entry.remaining;
        status = ApplyReport(OrderReport{
            std::move(order), static_cast<OrderStatus>(entry.result),
            entry.message, entry.transaction_ms, entry.credit});
        break;
      }
      case JournalKind::Fill:
        status = ApplyFill(Fill{{}, entry.side == 0 ? Side::Buy : Side::Sell,
                                entry.trade_id, entry.price, entry.quantity,
                                entry.gross, entry.fee, MarketId{entry.symbol},
                                id, entry.fee_asset, entry.transaction_ms});
        break;
      case JournalKind::SubmitIntent:
      case JournalKind::Balance:
      case JournalKind::BalanceDelta:
      case JournalKind::Control:
        break;
    }
    if (!status.ok()) return status;
  }
  return absl::OkStatus();
}

}  // namespace hquant::v1
