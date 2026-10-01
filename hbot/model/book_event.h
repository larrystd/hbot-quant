#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "hbot/base/clock.h"
#include "hbot/model/market.h"

namespace hbot {

enum class Side { Buy, Sell };

struct BookLevel {
  PriceTicks price_ticks;
  QuantityLots quantity_lots;
};

struct BookSnapshot {
  MarketId market;
  uint64_t scale_version = 0;
  uint64_t stream_epoch = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
  EventTime time;
};

struct BookDiff {
  MarketId market;
  uint64_t scale_version = 0;
  uint64_t stream_epoch = 0;
  uint64_t first_sequence = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
  EventTime time;
};

struct PublicTrade {
  MarketId market;
  std::optional<std::string> public_trade_id;
  PriceTicks price_ticks;
  QuantityLots quantity_lots;
  std::optional<Side> side;
  EventTime time;
};

}  // namespace hbot
