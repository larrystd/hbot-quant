#include "hbot/connector/binance_spot/public/depth_parser.h"

#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "hbot/base/decimal.h"
#include "simdjson.h"

namespace hbot::binance_spot {
namespace {

absl::Status Invalid(std::string_view reason) {
  return absl::InvalidArgumentError(std::string(reason));
}

absl::StatusOr<std::string_view> Text(simdjson::dom::element object,
                                      const char* key) {
  std::string_view value;
  if (object[key].get(value))
    return Invalid(std::string("missing string ") + key);
  return value;
}

absl::StatusOr<uint64_t> Unsigned(simdjson::dom::element object,
                                  const char* key) {
  uint64_t value;
  if (object[key].get(value))
    return Invalid(std::string("missing unsigned ") + key);
  return value;
}

absl::StatusOr<simdjson::dom::element> ParseRoot(simdjson::dom::parser* parser,
                                                 std::string_view json) {
  simdjson::dom::element root;
  if (parser->parse(json).get(root)) return Invalid("invalid JSON");
  simdjson::dom::element data;
  if (!root["data"].get(data)) return data;
  return root;
}

// Decimal::ToString may use exponent notation after normalization. Parse it as
// an exact integer without any binary floating-point conversion.
absl::StatusOr<uint64_t> IntegerDecimal(std::string_view text) {
  if (text.empty() || text[0] == '-')
    return Invalid("negative integer decimal");
  const size_t exponent_at = text.find_first_of("eE");
  const auto mantissa = text.substr(0, exponent_at);
  int exponent = 0;
  if (exponent_at != std::string_view::npos) {
    auto exp = text.substr(exponent_at + 1);
    if (!exp.empty() && exp[0] == '+') exp.remove_prefix(1);
    const auto [end, ec] =
        std::from_chars(exp.data(), exp.data() + exp.size(), exponent);
    if (ec != std::errc{} || end != exp.data() + exp.size()) {
      return Invalid("invalid integer exponent");
    }
  }
  std::string digits;
  int fractional = 0;
  bool point = false;
  for (char character : mantissa) {
    if (character == '.') {
      if (point) return Invalid("invalid integer decimal");
      point = true;
    } else if (character >= '0' && character <= '9') {
      digits += character;
      if (point) ++fractional;
    } else
      return Invalid("invalid integer decimal");
  }
  if (digits.empty()) return Invalid("empty integer decimal");
  int shift = exponent - fractional;
  while (shift < 0 && !digits.empty() && digits.back() == '0') {
    digits.pop_back();
    ++shift;
  }
  if (shift < 0) return Invalid("non-integral scale quotient");
  if (shift > 20)
    return absl::OutOfRangeError("scale quotient overflows uint64");
  uint64_t value = 0;
  for (char digit : digits) {
    const unsigned d = digit - '0';
    if (value > (std::numeric_limits<uint64_t>::max() - d) / 10) {
      return absl::OutOfRangeError("scale quotient overflows uint64");
    }
    value = value * 10 + d;
  }
  for (int i = 0; i < shift; ++i) {
    if (value > std::numeric_limits<uint64_t>::max() / 10) {
      return absl::OutOfRangeError("scale quotient overflows uint64");
    }
    value *= 10;
  }
  return value;
}

absl::StatusOr<uint64_t> ToUnits(std::string_view raw, const Decimal& quantum) {
  auto value = Decimal::Parse(raw);
  if (!value.ok()) return value.status();
  if (!value->IsNonnegative()) return Invalid("negative level");
  auto rounded = value->Quantize(quantum, RoundingMode::Down);
  if (!rounded.ok()) return rounded.status();
  auto exact = rounded->Compare(*value);
  if (!exact.ok()) return exact.status();
  if (*exact != 0) return Invalid("raw level not divisible by BookScale");
  auto quotient = value->Divide(quantum);
  if (!quotient.ok()) return quotient.status();
  return IntegerDecimal(quotient->ToString());
}

absl::StatusOr<std::vector<BookLevel>> Levels(simdjson::dom::element object,
                                              const char* key,
                                              const BookScale& scale,
                                              bool allow_zero_quantity) {
  simdjson::dom::array raw_levels;
  if (object[key].get(raw_levels))
    return Invalid(std::string("missing levels ") + key);
  std::vector<BookLevel> levels;
  levels.reserve(raw_levels.size());
  for (auto raw_level : raw_levels) {
    simdjson::dom::array pair;
    if (raw_level.get(pair) || pair.size() != 2)
      return Invalid("invalid level pair");
    std::string_view price_text, quantity_text;
    if (pair.at(0).get(price_text) || pair.at(1).get(quantity_text)) {
      return Invalid("level price and quantity must be strings");
    }
    auto price = ToUnits(price_text, scale.quote_per_tick);
    auto quantity = ToUnits(quantity_text, scale.base_per_lot);
    if (!price.ok()) return price.status();
    if (!quantity.ok()) return quantity.status();
    if (*price == 0 || *price > uint64_t(std::numeric_limits<int64_t>::max()) ||
        (!allow_zero_quantity && *quantity == 0)) {
      return Invalid("level price or quantity out of range");
    }
    levels.push_back(
        BookLevel{PriceTicks{int64_t(*price)}, QuantityLots{*quantity}});
  }
  return levels;
}

void ExchangeEventTime(simdjson::dom::element object, EventTime* time) {
  uint64_t millis;
  if (!object["E"].get(millis) &&
      millis <= uint64_t(std::numeric_limits<int64_t>::max() / 1000)) {
    time->exchange_utc = UtcTime(std::chrono::microseconds(millis * 1000));
  }
}

}  // namespace

absl::StatusOr<BookSnapshot> DepthParser::ParseSnapshot(
    std::string_view json, uint64_t stream_epoch, EventTime received) const {
  if (!scale_.IsValid() || stream_epoch == 0)
    return Invalid("invalid BookScale or epoch");
  simdjson::dom::parser parser;
  auto parsed = ParseRoot(&parser, json);
  if (!parsed.ok()) return parsed.status();
  auto sequence = Unsigned(*parsed, "lastUpdateId");
  if (!sequence.ok() || *sequence == 0)
    return Invalid("invalid snapshot sequence");
  auto bids = Levels(*parsed, "bids", scale_, false);
  auto asks = Levels(*parsed, "asks", scale_, false);
  if (!bids.ok()) return bids.status();
  if (!asks.ok()) return asks.status();
  BookSnapshot snapshot;
  snapshot.market = market_;
  snapshot.scale_version = scale_.scale_version;
  snapshot.stream_epoch = stream_epoch;
  snapshot.last_sequence = *sequence;
  snapshot.bids = std::move(*bids);
  snapshot.asks = std::move(*asks);
  snapshot.time = received;
  return snapshot;
}

absl::StatusOr<BookDiff> DepthParser::ParseDiff(std::string_view json,
                                                uint64_t stream_epoch,
                                                EventTime received) const {
  if (!scale_.IsValid() || stream_epoch == 0)
    return Invalid("invalid BookScale or epoch");
  simdjson::dom::parser parser;
  auto parsed = ParseRoot(&parser, json);
  if (!parsed.ok()) return parsed.status();
  auto type = Text(*parsed, "e");
  auto symbol = Text(*parsed, "s");
  if (!type.ok() || *type != "depthUpdate" || !symbol.ok() ||
      *symbol != market_.native_symbol) {
    return Invalid("not a depthUpdate for configured market");
  }
  auto first = Unsigned(*parsed, "U");
  auto last = Unsigned(*parsed, "u");
  if (!first.ok() || !last.ok() || *first == 0 || *first > *last) {
    return Invalid("invalid depth interval");
  }
  auto bids = Levels(*parsed, "b", scale_, true);
  auto asks = Levels(*parsed, "a", scale_, true);
  if (!bids.ok()) return bids.status();
  if (!asks.ok()) return asks.status();
  ExchangeEventTime(*parsed, &received);
  BookDiff diff;
  diff.market = market_;
  diff.scale_version = scale_.scale_version;
  diff.stream_epoch = stream_epoch;
  diff.first_sequence = *first;
  diff.last_sequence = *last;
  diff.bids = std::move(*bids);
  diff.asks = std::move(*asks);
  diff.time = received;
  return diff;
}

absl::StatusOr<PublicTrade> DepthParser::ParseTrade(std::string_view json,
                                                    EventTime received) const {
  if (!scale_.IsValid()) return Invalid("invalid BookScale");
  simdjson::dom::parser parser;
  auto parsed = ParseRoot(&parser, json);
  if (!parsed.ok()) return parsed.status();
  auto type = Text(*parsed, "e");
  auto symbol = Text(*parsed, "s");
  if (!type.ok() || (*type != "trade" && *type != "aggTrade") || !symbol.ok() ||
      *symbol != market_.native_symbol) {
    return Invalid("not a trade for configured market");
  }
  auto price_text = Text(*parsed, "p");
  auto quantity_text = Text(*parsed, "q");
  if (!price_text.ok()) return price_text.status();
  if (!quantity_text.ok()) return quantity_text.status();
  auto price = ToUnits(*price_text, scale_.quote_per_tick);
  auto quantity = ToUnits(*quantity_text, scale_.base_per_lot);
  if (!price.ok()) return price.status();
  if (!quantity.ok()) return quantity.status();
  if (*price == 0 || *price > uint64_t(std::numeric_limits<int64_t>::max()) ||
      *quantity == 0)
    return Invalid("invalid trade price or quantity");
  bool buyer_maker = false;
  if ((*parsed)["m"].get(buyer_maker))
    return Invalid("missing trade maker flag");
  auto trade_id = Unsigned(*parsed, *type == "trade" ? "t" : "a");
  if (!trade_id.ok()) return trade_id.status();
  ExchangeEventTime(*parsed, &received);
  PublicTrade trade;
  trade.market = market_;
  trade.public_trade_id = std::to_string(*trade_id);
  trade.price_ticks = PriceTicks{int64_t(*price)};
  trade.quantity_lots = QuantityLots{*quantity};
  trade.side = buyer_maker ? Side::Sell : Side::Buy;
  trade.time = received;
  return trade;
}

}  // namespace hbot::binance_spot
