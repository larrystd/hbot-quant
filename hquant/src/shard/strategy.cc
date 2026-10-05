#include "hquant/shard/strategy.h"

#include <chrono>
#include <utility>

#include "absl/status/status.h"
#include "hquant/base/fixed.h"

namespace hquant::v1 {

absl::StatusOr<StrategyDecision> Strategy::Decide(
    const BookView& book, const ExecutionView& execution, MonoTime now) {
  if (book.state != BookState::Live) {
    next_refresh_at_.reset();
    if (!execution.active_order_ids.empty()) {
      return StrategyDecision(CancelOrders{execution.active_order_ids});
    }
    return StrategyDecision(NoAction{});
  }
  if (next_refresh_at_ && now < *next_refresh_at_) {
    return StrategyDecision(NoAction{});
  }
  if (!execution.active_order_ids.empty()) {
    return StrategyDecision(CancelOrders{execution.active_order_ids});
  }

  next_refresh_at_ = now + std::chrono::milliseconds(config_.refresh_ms);
  if (!book.best_bid || !book.best_ask ||
      book.best_bid->price_ticks >= book.best_ask->price_ticks) {
    return StrategyDecision(NoAction{});
  }
  auto bid = MulDivFloor(book.best_bid->price_ticks,
                         execution.price_per_tick_nanos, 1);
  auto ask = MulDivFloor(book.best_ask->price_ticks,
                         execution.price_per_tick_nanos, 1);
  if (!bid.ok()) return bid.status();
  if (!ask.ok()) return ask.status();
  const uint64_t reference = *bid + (*ask - *bid) / 2;
  auto raw_bid = MulDivFloor(reference, kFixedScale - config_.bid_spread_ppb,
                             kFixedScale);
  auto raw_ask = MulDivCeil(reference, kFixedScale + config_.ask_spread_ppb,
                            kFixedScale);
  if (!raw_bid.ok()) return raw_bid.status();
  if (!raw_ask.ok()) return raw_ask.status();
  const uint64_t bid_price = *raw_bid / execution.price_increment_nanos *
                             execution.price_increment_nanos;
  auto ask_price = RoundUp(*raw_ask, execution.price_increment_nanos);
  if (!ask_price.ok()) return ask_price.status();
  const uint64_t quantity = config_.order_amount_nanos /
                            execution.base_increment_nanos *
                            execution.base_increment_nanos;
  if (quantity < execution.min_base_amount_nanos || bid_price == 0 ||
      *ask_price <= bid_price) {
    return StrategyDecision(NoAction{});
  }

  SubmitOrders submit;
  auto buy_gross = MulDivFloor(bid_price, quantity, kFixedScale);
  auto sell_gross = MulDivFloor(*ask_price, quantity, kFixedScale);
  if (!buy_gross.ok()) return buy_gross.status();
  if (!sell_gross.ok()) return sell_gross.status();
  auto buy_fee = MulDivCeil(*buy_gross, execution.maker_fee_rate_ppb,
                            kFixedScale);
  if (!buy_fee.ok()) return buy_fee.status();
  auto buy_cost = CheckedAdd(*buy_gross, *buy_fee);
  if (!buy_cost.ok()) return buy_cost.status();
  if (*buy_gross >= execution.min_order_value_nanos &&
      *buy_cost <= execution.quote_budget_nanos) {
    submit.orders.push_back(
        LimitOrderRequest{Side::Buy, bid_price, quantity});
  }
  if (*sell_gross >= execution.min_order_value_nanos &&
      quantity <= execution.base_budget_nanos) {
    submit.orders.push_back(
        LimitOrderRequest{Side::Sell, *ask_price, quantity});
  }
  if (submit.orders.empty()) return StrategyDecision(NoAction{});
  return StrategyDecision(std::move(submit));
}

}  // namespace hquant::v1
