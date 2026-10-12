#include "hquant/trading/risk_limits.h"

#include <limits>

#include "hquant/base/fixed.h"

namespace hquant::v1 {

absl::Status RiskLimits::Check(
    const AccountConfig::RiskPolicy& policy, const MarketConfig& market,
    const LimitOrderRequest& request,
    const std::map<uint16_t, MarketConfig>& markets,
    const OrderState& orders, const VenueRule* venue_rule) {
  if (request.price_nanos == 0 || request.quantity_nanos == 0 ||
      request.price_nanos % market.rule.price_increment_nanos != 0 ||
      request.quantity_nanos % market.rule.base_increment_nanos != 0 ||
      request.quantity_nanos < market.rule.min_base_amount_nanos) {
    return absl::InvalidArgumentError("order violates price or quantity rule");
  }
  auto value = MulDivCeil(request.price_nanos, request.quantity_nanos,
                          kFixedScale);
  if (!value.ok()) return value.status();
  if (*value < market.rule.min_order_value_nanos) {
    return absl::InvalidArgumentError("order below minimum value");
  }
  if (venue_rule &&
      (!venue_rule->trading ||
       (venue_rule->min_price_nanos != 0 &&
        request.price_nanos < venue_rule->min_price_nanos) ||
       (request.price_nanos - venue_rule->min_price_nanos) %
               venue_rule->price_increment_nanos != 0 ||
       (venue_rule->max_price_nanos != 0 &&
        request.price_nanos > venue_rule->max_price_nanos) ||
       request.quantity_nanos < venue_rule->min_base_amount_nanos ||
       (request.quantity_nanos - venue_rule->min_base_amount_nanos) %
               venue_rule->base_increment_nanos != 0 ||
       (venue_rule->max_base_amount_nanos != 0 &&
        request.quantity_nanos > venue_rule->max_base_amount_nanos) ||
       *value < venue_rule->min_order_value_nanos ||
       (venue_rule->max_order_value_nanos != 0 &&
        *value > venue_rule->max_order_value_nanos))) {
    return absl::InvalidArgumentError("order violates current venue rule");
  }
  if (policy.max_order_value_nanos != 0 &&
      *value > policy.max_order_value_nanos) {
    return absl::ResourceExhaustedError("order value risk limit exceeded");
  }
  if (policy.max_active_orders == 0 &&
      policy.max_symbol_active_orders == 0 &&
      policy.max_open_value_per_quote_nanos == 0) {
    return absl::OkStatus();
  }

  uint64_t active = 0;
  uint64_t symbol_active = 0;
  uint64_t quote_open_value = *value;
  for (const auto& [id, order] : orders.Orders()) {
    if (order.Terminal()) continue;
    ++active;
    if (order.market == market.id) ++symbol_active;
    if (policy.max_open_value_per_quote_nanos == 0) continue;
    const auto existing_market = markets.find(order.shard.value);
    if (existing_market == markets.end()) {
      return absl::DataLossError("active order market missing");
    }
    if (existing_market->second.quote_asset != market.quote_asset) continue;
    auto existing_value = MulDivCeil(order.price_nanos, order.Remaining(),
                                     kFixedScale);
    if (!existing_value.ok()) return existing_value.status();
    if (*existing_value >
        std::numeric_limits<uint64_t>::max() - quote_open_value) {
      return absl::OutOfRangeError("open order value overflow");
    }
    quote_open_value += *existing_value;
  }
  if (policy.max_active_orders != 0 &&
      active >= policy.max_active_orders) {
    return absl::ResourceExhaustedError("account active order limit reached");
  }
  if (policy.max_symbol_active_orders != 0 &&
      symbol_active >= policy.max_symbol_active_orders) {
    return absl::ResourceExhaustedError("symbol active order limit reached");
  }
  if (policy.max_open_value_per_quote_nanos != 0 &&
      quote_open_value > policy.max_open_value_per_quote_nanos) {
    return absl::ResourceExhaustedError("quote open value risk limit exceeded");
  }
  return absl::OkStatus();
}

}  // namespace hquant::v1
