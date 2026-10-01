#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/types.h"

namespace hquant {

enum class InstrumentKind { Spot };

struct MarketId {
  ExchangeId exchange;
  InstrumentKind instrument_kind = InstrumentKind::Spot;
  std::string native_symbol;
  bool operator==(const MarketId&) const = default;

  template <typename H>
  friend H AbslHashValue(H hash, const MarketId& market) {
    return H::combine(std::move(hash), market.exchange, market.instrument_kind,
                      market.native_symbol);
  }
};

struct MarketSpec {
  MarketId market;
  AssetId base_asset;
  AssetId quote_asset;
};

struct PriceTicks {
  int64_t value = 0;
  bool operator==(const PriceTicks&) const = default;
};
struct QuantityLots {
  uint64_t value = 0;
  bool operator==(const QuantityLots&) const = default;
};

struct TickLotSize {
  Decimal price_per_tick;
  Decimal amount_per_lot;
  uint64_t tick_lot_version = 0;
  bool IsValid() const {
    return price_per_tick.IsStrictlyPositive() &&
           amount_per_lot.IsStrictlyPositive() && tick_lot_version > 0;
  }
};

enum class Side { Buy, Sell };

struct BookLevel {
  PriceTicks price_ticks;
  QuantityLots quantity_lots;
};

struct BookSnapshot {
  MarketId market;
  uint64_t tick_lot_version = 0;
  uint64_t connection_id = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
  EventTime time;
};

struct BookDiff {
  MarketId market;
  uint64_t tick_lot_version = 0;
  uint64_t connection_id = 0;
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

}  // namespace hquant
