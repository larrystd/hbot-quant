#include "hquant/shard/paper_exchange.h"

#include <algorithm>
#include <limits>
#include <set>
#include <utility>

#include "absl/status/status.h"
#include "hquant/base/fixed.h"

namespace hquant::v1 {

PaperExchange::PaperExchange(const MarketConfig& market,
                             const ExecutorConfig& config,
                             EventSink event_sink)
    : market_(market), config_(config),
      totals_(config.initial_balances_nanos),
      event_sink_(std::move(event_sink)) {}

std::string PaperExchange::CollateralAsset(Side side) const {
  return side == Side::Buy ? market_.quote_asset : market_.base_asset;
}

uint64_t PaperExchange::Total(const std::string& asset) const {
  const auto found = totals_.find(asset);
  return found == totals_.end() ? 0 : found->second;
}

uint64_t PaperExchange::Available(const std::string& asset) const {
  uint64_t frozen = 0;
  for (const auto& [id, order] : active_orders_) {
    if (CollateralAsset(order.side) != asset) continue;
    if (frozen > std::numeric_limits<uint64_t>::max() - order.reserved_nanos) {
      return 0;
    }
    frozen += order.reserved_nanos;
  }
  return frozen > Total(asset) ? 0 : Total(asset) - frozen;
}

BalanceUpdate PaperExchange::BalanceOf(const std::string& asset) const {
  return BalanceUpdate{asset, Total(asset), Available(asset)};
}

absl::StatusOr<uint64_t> PaperExchange::BuyReserve(
    uint64_t price_nanos, uint64_t quantity_nanos) const {
  auto gross = MulDivCeil(price_nanos, quantity_nanos, kFixedScale);
  if (!gross.ok()) return gross.status();
  auto fee = MulDivCeil(*gross, config_.maker_fee_rate_ppb, kFixedScale);
  if (!fee.ok()) return fee.status();
  auto reserve = CheckedAdd(*gross, *fee);
  if (!reserve.ok()) return reserve.status();
  // A fee can round up once per partial fill. One nano per possible lot
  // bounds that extra charge without assuming an order fills in one event.
  if (config_.maker_fee_rate_ppb == 0) return *reserve;
  return CheckedAdd(*reserve,
                    quantity_nanos / market_.rule.base_increment_nanos);
}

absl::StatusOr<uint64_t> PaperExchange::LotsToQuantity(uint64_t lots) const {
  auto quantity = MulDivFloor(lots, market_.amount_per_lot_nanos, 1);
  if (!quantity.ok()) return quantity.status();
  return *quantity / market_.rule.base_increment_nanos *
         market_.rule.base_increment_nanos;
}

absl::StatusOr<OrderId> PaperExchange::Place(
    const LimitOrderRequest& request) {
  if (request.price_nanos == 0 || request.quantity_nanos == 0) {
    return absl::InvalidArgumentError("paper order price or quantity is zero");
  }
  if (next_order_id_ == std::numeric_limits<uint64_t>::max()) {
    return absl::OutOfRangeError("paper order id exhausted");
  }
  uint64_t reserve = request.quantity_nanos;
  if (request.side == Side::Buy) {
    auto value = BuyReserve(request.price_nanos, request.quantity_nanos);
    if (!value.ok()) return value.status();
    reserve = *value;
  }
  const OrderId id{next_order_id_++};
  Order order{id, request.side, request.price_nanos, request.quantity_nanos,
              request.quantity_nanos, reserve};
  const std::string collateral = CollateralAsset(request.side);
  if (Available(collateral) < reserve) {
    order.reserved_nanos = 0;
    event_sink_(OrderReport{
        order, OrderState::Rejected, "insufficient exchange balance"});
    return id;
  }
  active_orders_.emplace(id.value, order);
  event_sink_(OrderReport{order, OrderState::Accepted});
  event_sink_(BalanceOf(collateral));
  return id;
}

absl::Status PaperExchange::Cancel(const std::vector<OrderId>& ids) {
  std::set<uint64_t> unique;
  for (OrderId id : ids) {
    if (!unique.insert(id.value).second || !active_orders_.contains(id.value)) {
      return absl::FailedPreconditionError("cancel order missing or duplicated");
    }
  }
  std::vector<AccountEvent> events;
  events.reserve(ids.size() + 2);
  for (OrderId id : ids) {
    Order order = active_orders_.at(id.value);
    active_orders_.erase(id.value);
    events.push_back(OrderReport{order, OrderState::Cancelled});
  }
  if (!ids.empty()) {
    events.push_back(BalanceOf(market_.base_asset));
    events.push_back(BalanceOf(market_.quote_asset));
  }
  for (AccountEvent& event : events) event_sink_(std::move(event));
  return absl::OkStatus();
}

absl::Status PaperExchange::CancelAll() {
  std::vector<OrderId> ids;
  ids.reserve(active_orders_.size());
  for (const auto& [id, order] : active_orders_) ids.push_back(OrderId{id});
  return Cancel(ids);
}

absl::Status PaperExchange::FillMatches(const std::vector<Match>& matches) {
  if (matches.empty()) return absl::OkStatus();
  auto next_totals = totals_;
  auto next_orders = active_orders_;
  uint64_t next_trade_id = next_trade_id_;
  std::vector<AccountEvent> events;
  events.reserve(matches.size() * 2 + 2);
  for (const Match& match : matches) {
    auto found = next_orders.find(match.id.value);
    if (found == next_orders.end() || match.quantity_nanos == 0 ||
        match.quantity_nanos > found->second.remaining_nanos ||
        match.price_nanos == 0) {
      return absl::InternalError("invalid paper fill");
    }
    if (next_trade_id == std::numeric_limits<uint64_t>::max()) {
      return absl::OutOfRangeError("paper trade id exhausted");
    }
    Order& order = found->second;
    auto gross = MulDivFloor(match.price_nanos, match.quantity_nanos,
                             kFixedScale);
    if (!gross.ok()) return gross.status();
    auto fee = MulDivCeil(*gross, config_.maker_fee_rate_ppb, kFixedScale);
    if (!fee.ok()) return fee.status();
    if (order.side == Side::Buy) {
      auto cost = CheckedAdd(*gross, *fee);
      if (!cost.ok()) return cost.status();
      auto base = CheckedAdd(next_totals[market_.base_asset],
                             match.quantity_nanos);
      auto quote = CheckedSubtract(next_totals[market_.quote_asset], *cost);
      if (!base.ok()) return base.status();
      if (!quote.ok()) return quote.status();
      next_totals[market_.base_asset] = *base;
      next_totals[market_.quote_asset] = *quote;
    } else {
      auto base = CheckedSubtract(next_totals[market_.base_asset],
                                   match.quantity_nanos);
      auto proceeds = CheckedSubtract(*gross, *fee);
      if (!base.ok()) return base.status();
      if (!proceeds.ok()) return proceeds.status();
      auto quote = CheckedAdd(next_totals[market_.quote_asset], *proceeds);
      if (!quote.ok()) return quote.status();
      next_totals[market_.base_asset] = *base;
      next_totals[market_.quote_asset] = *quote;
    }
    order.remaining_nanos -= match.quantity_nanos;
    if (order.side == Side::Buy) {
      auto reserve = BuyReserve(order.price_nanos, order.remaining_nanos);
      if (!reserve.ok()) return reserve.status();
      order.reserved_nanos = *reserve;
    } else {
      order.reserved_nanos = order.remaining_nanos;
    }
    const Order report = order;
    events.push_back(Fill{order.id, order.side, next_trade_id++,
                          match.price_nanos, match.quantity_nanos,
                          *gross, *fee});
    events.push_back(OrderReport{
        report, report.remaining_nanos == 0
                    ? OrderState::Filled : OrderState::PartiallyFilled});
    if (report.remaining_nanos == 0) next_orders.erase(found);
  }
  for (const std::string& asset : {market_.base_asset, market_.quote_asset}) {
    uint64_t frozen = 0;
    for (const auto& [id, order] : next_orders) {
      if (CollateralAsset(order.side) != asset) continue;
      auto total = CheckedAdd(frozen, order.reserved_nanos);
      if (!total.ok()) return total.status();
      frozen = *total;
    }
    if (frozen > next_totals[asset]) {
      return absl::InternalError("paper balance below active order reserve");
    }
  }
  totals_ = std::move(next_totals);
  active_orders_ = std::move(next_orders);
  next_trade_id_ = next_trade_id;
  events.push_back(BalanceOf(market_.base_asset));
  events.push_back(BalanceOf(market_.quote_asset));
  for (AccountEvent& event : events) event_sink_(std::move(event));
  return absl::OkStatus();
}

absl::Status PaperExchange::OnMarket(const MarketNotice& notice,
                                     const BookView& book) {
  switch (notice.kind) {
    case MarketNotice::Kind::BookChanged:
      return OnBookBbo(book);
    case MarketNotice::Kind::PublicTrade:
      if (!notice.trade) return absl::InternalError("public trade notice missing trade");
      return OnPublicTrade(*notice.trade);
    case MarketNotice::Kind::BookUnavailable:
      return absl::OkStatus();
  }
  return absl::InternalError("unknown market notice");
}

absl::Status PaperExchange::OnBookBbo(const BookView& book) {
  if (book.state != BookState::Live || !book.best_bid || !book.best_ask) {
    return absl::InvalidArgumentError("paper BBO unavailable");
  }
  auto bid = MulDivFloor(book.best_bid->price_ticks,
                         market_.price_per_tick_nanos, 1);
  auto ask = MulDivFloor(book.best_ask->price_ticks,
                         market_.price_per_tick_nanos, 1);
  auto bid_quantity = LotsToQuantity(book.best_bid->quantity_lots);
  auto ask_quantity = LotsToQuantity(book.best_ask->quantity_lots);
  if (!bid.ok()) return bid.status();
  if (!ask.ok()) return ask.status();
  if (!bid_quantity.ok()) return bid_quantity.status();
  if (!ask_quantity.ok()) return ask_quantity.status();
  uint64_t bid_available = *bid_quantity;
  uint64_t ask_available = *ask_quantity;
  std::vector<Match> matches;
  for (const auto& [id, order] : active_orders_) {
    if (order.side == Side::Buy && order.price_nanos >= *ask) {
      const uint64_t quantity = std::min(order.remaining_nanos, ask_available);
      if (quantity == 0) continue;
      matches.push_back(Match{order.id, quantity, *ask});
      ask_available -= quantity;
    } else if (order.side == Side::Sell && order.price_nanos <= *bid) {
      const uint64_t quantity = std::min(order.remaining_nanos, bid_available);
      if (quantity == 0) continue;
      matches.push_back(Match{order.id, quantity, *bid});
      bid_available -= quantity;
    }
  }
  return FillMatches(matches);
}

absl::Status PaperExchange::OnPublicTrade(const PublicTrade& trade) {
  if (trade.price_ticks == 0 || trade.quantity_lots == 0) {
    return absl::InvalidArgumentError("paper public trade invalid");
  }
  auto price = MulDivFloor(trade.price_ticks, market_.price_per_tick_nanos, 1);
  auto quantity = LotsToQuantity(trade.quantity_lots);
  if (!price.ok()) return price.status();
  if (!quantity.ok()) return quantity.status();
  uint64_t available = *quantity;
  std::vector<Match> matches;
  for (const auto& [id, order] : active_orders_) {
    if (order.side == trade.aggressor) continue;
    if ((order.side == Side::Buy && order.price_nanos >= *price) ||
        (order.side == Side::Sell && order.price_nanos <= *price)) {
      const uint64_t fill = std::min(order.remaining_nanos, available);
      if (fill == 0) continue;
      matches.push_back(Match{order.id, fill, order.price_nanos});
      available -= fill;
    }
  }
  return FillMatches(matches);
}

}  // namespace hquant::v1
