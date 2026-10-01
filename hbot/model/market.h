#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"

namespace hbot {

enum class InstrumentKind { Spot };

struct MarketId {
  VenueId venue;
  InstrumentKind instrument_kind = InstrumentKind::Spot;
  std::string native_symbol;
  bool operator==(const MarketId&) const = default;

  template <typename H>
  friend H AbslHashValue(H hash, const MarketId& market) {
    return H::combine(std::move(hash), market.venue, market.instrument_kind,
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

struct BookScale {
  Decimal quote_per_tick;
  Decimal base_per_lot;
  uint64_t scale_version = 0;
  bool IsValid() const {
    return quote_per_tick.IsStrictlyPositive() &&
           base_per_lot.IsStrictlyPositive() && scale_version > 0;
  }
};

}  // namespace hbot
