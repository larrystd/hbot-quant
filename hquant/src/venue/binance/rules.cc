#include "hquant/venue/binance/rules.h"

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "hquant/base/fixed.h"
#include "simdjson.h"

namespace hquant::v1 {
namespace {

absl::StatusOr<std::string_view> Text(simdjson::dom::element row,
                                      const char* key) {
  std::string_view value;
  if (row[key].get(value)) {
    return absl::DataLossError(std::string("exchangeInfo missing ") + key);
  }
  return value;
}

absl::StatusOr<uint64_t> Fixed(simdjson::dom::element row, const char* key) {
  auto value = Text(row, key);
  if (!value.ok()) return value.status();
  return ParseFixed(*value);
}

}  // namespace

absl::StatusOr<std::vector<VenueRule>> ParseBinanceExchangeInfo(
    std::string_view json) {
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root)) {
    return absl::DataLossError("invalid exchangeInfo JSON");
  }
  simdjson::dom::array symbols;
  if (root["symbols"].get(symbols)) {
    return absl::DataLossError("exchangeInfo missing symbols");
  }
  std::vector<VenueRule> result;
  for (simdjson::dom::element symbol : symbols) {
    auto id = Text(symbol, "symbol");
    auto base = Text(symbol, "baseAsset");
    auto quote = Text(symbol, "quoteAsset");
    auto status = Text(symbol, "status");
    if (!id.ok()) return id.status();
    if (!base.ok()) return base.status();
    if (!quote.ok()) return quote.status();
    if (!status.ok()) return status.status();
    VenueRule rule;
    rule.market = MarketId{std::string(*id)};
    rule.base_asset = *base;
    rule.quote_asset = *quote;
    rule.trading = *status == "TRADING";
    bool price = false, lot = false, notional = false;
    simdjson::dom::array filters;
    if (symbol["filters"].get(filters)) {
      return absl::DataLossError("exchangeInfo missing filters");
    }
    for (simdjson::dom::element filter : filters) {
      auto kind = Text(filter, "filterType");
      if (!kind.ok()) return kind.status();
      if (*kind == "PRICE_FILTER") {
        if (price) return absl::DataLossError("duplicate PRICE_FILTER");
        price = true;
        auto increment = Fixed(filter, "tickSize");
        auto minimum = Fixed(filter, "minPrice");
        auto maximum = Fixed(filter, "maxPrice");
        if (!increment.ok()) return increment.status();
        if (!minimum.ok()) return minimum.status();
        if (!maximum.ok()) return maximum.status();
        rule.price_increment_nanos = *increment;
        rule.min_price_nanos = *minimum;
        rule.max_price_nanos = *maximum;
      } else if (*kind == "LOT_SIZE") {
        if (lot) return absl::DataLossError("duplicate LOT_SIZE");
        lot = true;
        auto increment = Fixed(filter, "stepSize");
        auto minimum = Fixed(filter, "minQty");
        auto maximum = Fixed(filter, "maxQty");
        if (!increment.ok()) return increment.status();
        if (!minimum.ok()) return minimum.status();
        if (!maximum.ok()) return maximum.status();
        rule.base_increment_nanos = *increment;
        rule.min_base_amount_nanos = *minimum;
        rule.max_base_amount_nanos = *maximum;
      } else if (*kind == "MIN_NOTIONAL" || *kind == "NOTIONAL") {
        auto minimum = Fixed(filter, "minNotional");
        if (!minimum.ok()) return minimum.status();
        rule.min_order_value_nanos =
            std::max(rule.min_order_value_nanos, *minimum);
        notional = true;
        if (*kind == "NOTIONAL") {
          auto maximum = Fixed(filter, "maxNotional");
          if (!maximum.ok()) return maximum.status();
          rule.max_order_value_nanos = *maximum;
        }
      }
    }
    if (!price || !lot || !notional || rule.price_increment_nanos == 0 ||
        rule.base_increment_nanos == 0 ||
        rule.min_base_amount_nanos == 0 ||
        rule.min_order_value_nanos == 0 ||
        (rule.max_price_nanos != 0 &&
         rule.max_price_nanos < rule.min_price_nanos) ||
        (rule.max_base_amount_nanos != 0 &&
         rule.max_base_amount_nanos < rule.min_base_amount_nanos) ||
        (rule.max_order_value_nanos != 0 &&
         rule.max_order_value_nanos < rule.min_order_value_nanos)) {
      return absl::DataLossError("incomplete exchangeInfo trading rule");
    }
    result.push_back(std::move(rule));
  }
  return result;
}

}  // namespace hquant::v1
