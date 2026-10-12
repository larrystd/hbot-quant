#pragma once

#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "hquant/trading/trade_port.h"

namespace hquant::v1 {

absl::StatusOr<std::vector<VenueRule>> ParseBinanceExchangeInfo(
    std::string_view json);

}  // namespace hquant::v1
