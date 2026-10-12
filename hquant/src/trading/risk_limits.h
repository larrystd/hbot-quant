#pragma once

#include <cstdint>
#include <map>

#include "absl/status/status.h"
#include "hquant/config.h"
#include "hquant/trading/order_state.h"
#include "hquant/trading/trade_port.h"

namespace hquant::v1 {

// Checks a proposed order against all unfinished orders of one account.
class RiskLimits {
 public:
  static absl::Status Check(const AccountConfig::RiskPolicy& policy,
                            const MarketConfig& market,
                            const LimitOrderRequest& request,
                            const std::map<uint16_t, MarketConfig>& markets,
                            const OrderState& orders,
                            const VenueRule* venue_rule = nullptr);
};

}  // namespace hquant::v1
