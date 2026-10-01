#pragma once

#include "hbot/base/clock.h"
#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"

namespace hbot {

struct Balance {
  AccountId account;
  AssetId asset;
  Decimal total;
  Decimal venue_available;
  EventTime time;
};

using BalanceUpdate = Balance;

}  // namespace hbot
