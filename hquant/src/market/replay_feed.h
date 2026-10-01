#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "base/market.h"
#include "base/types.h"

namespace hquant {

struct ReplaySubscribe {
  uint64_t connection_id = 0;
};

struct ReplaySnapshot {
  uint64_t connection_id = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
};

struct ReplayDiff {
  uint64_t connection_id = 0;
  uint64_t first_sequence = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
};

struct ReplayTimer {};

struct ReplayPublicTrade {
  PriceTicks price_ticks;
  QuantityLots quantity_lots;
  Side side = Side::Buy;
};

using ReplayPayload = std::variant<ReplaySubscribe, ReplaySnapshot, ReplayDiff,
                                   ReplayTimer, ReplayPublicTrade>;

struct ReplayInput {
  InputTime stamp;
  ReplayPayload payload;
};

using ReplaySink = std::function<absl::Status(const ReplayInput&)>;

// Reads the fixture in input order and delivers each normalized market input.
absl::Status ReadReplayFile(const std::string& path, const ReplaySink& sink);

}  // namespace hquant
