#include "strategy/simple_pmm.h"

#include <limits>
#include <string>
#include <utility>

namespace hquant {
namespace {

Decimal D(std::string_view value) { return *Decimal::Parse(value); }

}  // namespace

TriggerPolicy SimplePmm::Triggers() const {
  TriggerPolicy policy;
  policy.book_mode = BookTriggerMode::BboChanged;
  policy.timer_period = config_.timer_period;
  return policy;
}

Decimal SimplePmm::Available(const StrategyInput& context,
                             const AssetId& asset) const {
  for (const auto& balance : context.balances) {
    if (balance.account == config_.account && balance.asset == asset) {
      return balance.available;
    }
  }
  return D("0");
}

std::optional<Decimal> SimplePmm::ReferencePrice(
    const StrategyInput& context) const {
  if (config_.price_type == PmmPriceType::Last) return context.last_trade_price;
  const auto bid = context.book.BestBid();
  const auto ask = context.book.BestAsk();
  if (!bid || !ask || bid->price_ticks.value <= 0 ||
      ask->price_ticks.value <= 0 || !context.tick_lot_size.IsValid())
    return std::nullopt;
  auto bid_price = D(std::to_string(bid->price_ticks.value))
                       .Multiply(context.tick_lot_size.price_per_tick);
  auto ask_price = D(std::to_string(ask->price_ticks.value))
                       .Multiply(context.tick_lot_size.price_per_tick);
  if (!bid_price.ok() || !ask_price.ok()) return std::nullopt;
  auto sum = bid_price->Add(*ask_price);
  if (!sum.ok()) return std::nullopt;
  auto midpoint = sum->Divide(D("2"));
  if (!midpoint.ok()) return std::nullopt;
  return *midpoint;
}

ActionBatch SimplePmm::Decide(const StrategyInput& context, Trigger /*why*/) {
  ActionBatch actions;
  // StrategyV2Base.tick only initializes on the first ready transition.
  if (!ready_to_trade_) {
    if (context.ready) ready_to_trade_ = true;
    return actions;
  }
  const int64_t now = context.input.at_us;
  if (now < 0 || (next_refresh_at_us_ && now < *next_refresh_at_us_))
    return actions;

  for (const Order* order : context.orders)
    actions.ordered.emplace_back(CancelOrder{order->client_order_id});

  const auto reference = ReferencePrice(context);
  if (!reference || !reference->IsStrictlyPositive()) return actions;
  auto buy_factor = D("1").Subtract(config_.bid_spread);
  auto sell_factor = D("1").Add(config_.ask_spread);
  if (!buy_factor.ok() || !sell_factor.ok() ||
      !buy_factor->IsStrictlyPositive())
    return actions;
  auto buy_price = reference->Multiply(*buy_factor);
  auto sell_price = reference->Multiply(*sell_factor);
  if (!buy_price.ok() || !sell_price.ok() || !buy_price->IsStrictlyPositive() ||
      !sell_price->IsStrictlyPositive()) {
    return actions;
  }

  Decimal buy_amount = config_.order_amount;
  Decimal sell_amount = config_.order_amount;
  auto buy_cost = buy_price->Multiply(buy_amount);
  if (!buy_cost.ok()) return actions;
  if (!config_.buy_fee_from_returns) {
    auto fee = buy_cost->Multiply(config_.maker_fee_rate);
    if (!fee.ok()) return actions;
    buy_cost = buy_cost->Add(*fee);
    if (!buy_cost.ok()) return actions;
  }
  auto buy_budget =
      Available(context, config_.market.quote_asset).Compare(*buy_cost);
  auto sell_budget =
      Available(context, config_.market.base_asset).Compare(sell_amount);
  if (!buy_budget.ok() || !sell_budget.ok()) return actions;
  // Python's all_or_none is per candidate. A zero candidate still reaches
  // place_order; Dispatcher/RiskGate later rejects the invalid amount.
  if (*buy_budget < 0) buy_amount = D("0");
  if (*sell_budget < 0) sell_amount = D("0");

  actions.ordered.emplace_back(
      SubmitOrder{Side::Buy, buy_amount, *buy_price, TimeInForce::Gtc});
  actions.ordered.emplace_back(
      SubmitOrder{Side::Sell, sell_amount, *sell_price, TimeInForce::Gtc});

  const int64_t period = config_.refresh_interval.count();
  if (period > 0 && now <= std::numeric_limits<int64_t>::max() - period) {
    next_refresh_at_us_ = now + period;
  } else {
    next_refresh_at_us_.reset();
  }
  return actions;
}

}  // namespace hquant
