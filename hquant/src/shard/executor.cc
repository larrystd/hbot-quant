#include "hquant/shard/executor.h"

#include <algorithm>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "hquant/base/fixed.h"

namespace hquant::v1 {

Executor::Executor(const MarketConfig& market, const ExecutorConfig& config,
                   PaperExchange& exchange)
    : market_(market), config_(config), exchange_(exchange) {}

uint64_t Executor::Budget(const std::string& asset) const {
  const uint64_t initial = config_.initial_balances_nanos.at(asset);
  const uint64_t hard = config_.hard_limits_nanos.at(asset);
  const uint64_t total = exchange_.Total(asset);
  const uint64_t available = exchange_.Available(asset);
  const uint64_t used = initial > total ? initial - total : 0;
  const uint64_t frozen = total > available ? total - available : 0;
  const uint64_t remaining = hard > used && hard - used > frozen
                                 ? hard - used - frozen : 0;
  return std::min(available, remaining);
}

ExecutionView Executor::View() const {
  ExecutionView view;
  for (const auto& [id, order] : exchange_.ActiveOrders()) {
    view.active_order_ids.push_back(OrderId{id});
  }
  view.base_available_nanos = exchange_.Available(market_.base_asset);
  view.quote_available_nanos = exchange_.Available(market_.quote_asset);
  view.base_budget_nanos = Budget(market_.base_asset);
  view.quote_budget_nanos = Budget(market_.quote_asset);
  view.maker_fee_rate_ppb = config_.maker_fee_rate_ppb;
  view.price_per_tick_nanos = market_.price_per_tick_nanos;
  view.price_increment_nanos = market_.rule.price_increment_nanos;
  view.base_increment_nanos = market_.rule.base_increment_nanos;
  view.min_base_amount_nanos = market_.rule.min_base_amount_nanos;
  view.min_order_value_nanos = market_.rule.min_order_value_nanos;
  return view;
}

std::string Executor::ValidateOrder(const LimitOrderRequest& request,
                                    const BookView& book) const {
  if (book.state != BookState::Live) return "book unavailable";
  if (request.price_nanos == 0 || request.quantity_nanos == 0) {
    return "price or quantity is zero";
  }
  if (request.price_nanos % market_.rule.price_increment_nanos != 0 ||
      request.quantity_nanos % market_.rule.base_increment_nanos != 0) {
    return "order not aligned to trading rule";
  }
  if (request.quantity_nanos < market_.rule.min_base_amount_nanos) {
    return "order below minimum quantity";
  }
  auto gross = MulDivFloor(request.price_nanos, request.quantity_nanos,
                           kFixedScale);
  if (!gross.ok()) return std::string(gross.status().message());
  if (*gross < market_.rule.min_order_value_nanos) {
    return "order below minimum value";
  }
  auto fee = MulDivCeil(*gross, config_.maker_fee_rate_ppb, kFixedScale);
  if (!fee.ok()) return std::string(fee.status().message());
  if (request.side == Side::Buy) {
    auto cost = CheckedAdd(*gross, *fee);
    if (!cost.ok()) return std::string(cost.status().message());
    if (*cost > Budget(market_.quote_asset)) return "quote budget exhausted";
  } else if (request.quantity_nanos > Budget(market_.base_asset)) {
    return "base budget exhausted";
  }
  return {};
}

absl::StatusOr<std::vector<CommandResult>> Executor::Dispatch(
    const StrategyDecision& decision, const BookView& book) {
  std::vector<CommandResult> result;
  if (std::holds_alternative<NoAction>(decision)) return result;
  if (const auto* cancel = std::get_if<CancelOrders>(&decision)) {
    for (OrderId id : cancel->ids) {
      auto found = exchange_.ActiveOrders().find(id.value);
      if (found == exchange_.ActiveOrders().end()) {
        result.push_back(CommandResult{CommandResult::Kind::Cancel, id,
                                       Side::Buy, 0, 0, false,
                                       "cancel order missing"});
        continue;
      }
      const Order order = found->second;
      if (auto status = exchange_.Cancel({id}); !status.ok()) return status;
      result.push_back(CommandResult{CommandResult::Kind::Cancel, id,
                                     order.side, order.price_nanos,
                                     order.remaining_nanos, true, {}});
    }
    return result;
  }
  const auto& submit = std::get<SubmitOrders>(decision);
  for (const auto& order : submit.orders) {
    CommandResult action{CommandResult::Kind::Submit, {}, order.side,
                         order.price_nanos, order.quantity_nanos, false, {}};
    action.reason = ValidateOrder(order, book);
    if (action.reason.empty()) {
      auto placed = exchange_.Place(order);
      if (!placed.ok()) return placed.status();
      action.dispatched = true;
      action.order_id = *placed;
    }
    result.push_back(std::move(action));
  }
  return result;
}

absl::StatusOr<std::vector<CommandResult>> Executor::CancelAll() {
  std::vector<CommandResult> result;
  result.reserve(exchange_.ActiveOrders().size());
  for (const auto& [id, order] : exchange_.ActiveOrders()) {
    result.push_back(CommandResult{CommandResult::Kind::Cancel, OrderId{id},
                                   order.side, order.price_nanos,
                                   order.remaining_nanos, true, {}});
  }
  if (auto status = exchange_.CancelAll(); !status.ok()) return status;
  return result;
}

}  // namespace hquant::v1
