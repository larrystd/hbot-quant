#pragma once

#include <cstdint>
#include <optional>

#include "hbot/base/clock.h"
#include "hbot/model/market.h"

namespace hbot {

struct TradingRule {
  MarketId market;
  Decimal price_increment;
  Decimal base_increment;
  Decimal min_base_amount;
  Decimal min_notional;
  std::optional<Decimal> max_base_amount;
  uint64_t revision = 0;
  UtcTime observed_at{};
};

}  // namespace hbot
