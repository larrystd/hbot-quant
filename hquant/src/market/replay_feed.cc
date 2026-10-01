#include "market/replay_feed.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"
#include "simdjson.h"

namespace hquant {
namespace {

absl::StatusOr<std::string> String(simdjson::dom::element object,
                                   const char* key) {
  std::string_view value;
  if (object[key].get(value))
    return Error(ErrorCode::kReplayFileInvalid, std::string("missing ") + key);
  return std::string(value);
}

absl::StatusOr<uint64_t> Unsigned(simdjson::dom::element object,
                                  const char* key) {
  uint64_t value = 0;
  if (object[key].get(value))
    return Error(ErrorCode::kReplayFileInvalid, std::string("missing ") + key);
  return value;
}

absl::StatusOr<int64_t> Signed(simdjson::dom::element object, const char* key) {
  int64_t value = 0;
  if (object[key].get(value))
    return Error(ErrorCode::kReplayFileInvalid, std::string("missing ") + key);
  return value;
}

absl::StatusOr<std::vector<BookLevel>> Levels(simdjson::dom::element object,
                                              const char* key) {
  simdjson::dom::array rows;
  if (object[key].get(rows))
    return Error(ErrorCode::kReplayFileInvalid, std::string("missing ") + key);
  std::vector<BookLevel> levels;
  for (auto row : rows) {
    simdjson::dom::array pair;
    if (row.get(pair) || pair.size() != 2)
      return Error(ErrorCode::kReplayFileInvalid, "invalid book level");
    int64_t price = 0;
    uint64_t amount = 0;
    if (pair.at(0).get(price) || pair.at(1).get(amount))
      return Error(ErrorCode::kReplayFileInvalid, "invalid book level number");
    levels.push_back({PriceTicks{price}, QuantityLots{amount}});
  }
  return levels;
}

}  // namespace

absl::Status ReadReplayFile(const std::string& path, const ReplaySink& sink) {
  simdjson::dom::parser parser;
  simdjson::padded_string json;
  if (simdjson::padded_string::load(path).get(json))
    return Error(ErrorCode::kReplayFileNotFound,
                 "replay fixture cannot be read: " + path);
  simdjson::dom::element root;
  if (parser.parse(json).get(root))
    return Error(ErrorCode::kReplayFileInvalid, "invalid replay JSON");
  uint64_t version = 0;
  if (root["schema_version"].get(version) || version != 1)
    return Error(ErrorCode::kReplayFileInvalid, "unsupported replay schema");
  simdjson::dom::array inputs;
  if (root["inputs"].get(inputs))
    return Error(ErrorCode::kReplayFileInvalid, "replay inputs missing");
  for (auto input : inputs) {
    auto at = Signed(input, "at_us");
    auto ordinal = Unsigned(input, "ordinal");
    auto kind = String(input, "kind");
    if (!at.ok()) return at.status();
    if (!ordinal.ok()) return ordinal.status();
    if (!kind.ok()) return kind.status();
    ReplayInput event{{*at, *ordinal}, ReplayTimer{}};
    if (*kind == "subscribe") {
      auto epoch = Unsigned(input, "stream_epoch");
      if (!epoch.ok()) return epoch.status();
      event.payload = ReplaySubscribe{*epoch};
    } else if (*kind == "snapshot" || *kind == "diff") {
      auto epoch = Unsigned(input, "stream_epoch");
      auto bids = Levels(input, "bids");
      auto asks = Levels(input, "asks");
      if (!epoch.ok()) return epoch.status();
      if (!bids.ok()) return bids.status();
      if (!asks.ok()) return asks.status();
      if (*kind == "snapshot") {
        auto sequence = Unsigned(input, "last_sequence");
        if (!sequence.ok()) return sequence.status();
        event.payload = ReplaySnapshot{*epoch, *sequence, std::move(*bids),
                                       std::move(*asks)};
      } else {
        auto first = Unsigned(input, "first_sequence");
        auto last = Unsigned(input, "last_sequence");
        if (!first.ok()) return first.status();
        if (!last.ok()) return last.status();
        event.payload = ReplayDiff{*epoch, *first, *last, std::move(*bids),
                                   std::move(*asks)};
      }
    } else if (*kind == "timer") {
      event.payload = ReplayTimer{};
    } else if (*kind == "public_trade") {
      auto price = Signed(input, "price_ticks");
      auto amount = Unsigned(input, "quantity_lots");
      auto side = String(input, "side");
      if (!price.ok()) return price.status();
      if (!amount.ok()) return amount.status();
      if (!side.ok()) return side.status();
      if (*side != "buy" && *side != "sell")
        return Error(ErrorCode::kReplayFileInvalid,
                     "invalid public trade side");
      event.payload =
          ReplayPublicTrade{PriceTicks{*price}, QuantityLots{*amount},
                            *side == "buy" ? Side::Buy : Side::Sell};
    } else {
      return Error(ErrorCode::kReplayFileInvalid, "unknown replay input kind");
    }
    auto status = sink(event);
    if (!status.ok()) return status;
  }
  return absl::OkStatus();
}

}  // namespace hquant
