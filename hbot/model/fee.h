#pragma once

#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"

namespace hbot {

struct TradeFee {
  AssetId asset;
  // Positive means fee paid; negative means rebate received.
  Decimal signed_amount;
};

}  // namespace hbot
