#include "hquant/replay_reader.h"

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "simdjson.h"

namespace hquant::v1 {
namespace {

using ShardIndex = std::array<const ShardConfig*, kMaxShards>;

absl::Status Invalid(std::string_view message) {
  return absl::InvalidArgumentError(std::string("replay: ") + std::string(message));
}

absl::StatusOr<std::string> String(simdjson::dom::element object,
                                   const char* key) {
  std::string_view value;
  if (object[key].get(value)) return Invalid(std::string("missing ") + key);
  return std::string(value);
}

absl::StatusOr<uint64_t> Unsigned(simdjson::dom::element object,
                                  const char* key) {
  uint64_t value = 0;
  if (object[key].get(value)) return Invalid(std::string("missing ") + key);
  return value;
}

absl::StatusOr<int64_t> Signed(simdjson::dom::element object,
                                const char* key) {
  int64_t value = 0;
  if (object[key].get(value)) return Invalid(std::string("missing ") + key);
  return value;
}

absl::StatusOr<std::vector<BookLevel>> Levels(simdjson::dom::element object,
                                              const char* key) {
  simdjson::dom::array rows;
  if (object[key].get(rows)) return Invalid(std::string("missing ") + key);
  std::vector<BookLevel> levels;
  levels.reserve(rows.size());
  for (auto row : rows) {
    simdjson::dom::array pair;
    if (row.get(pair) || pair.size() != 2) return Invalid("invalid book level");
    uint64_t price = 0;
    uint64_t quantity = 0;
    if (pair.at(0).get(price) || pair.at(1).get(quantity)) {
      return Invalid("invalid book level number");
    }
    levels.push_back(BookLevel{price, quantity});
  }
  return levels;
}

absl::StatusOr<ShardId> Target(simdjson::dom::element input,
                                const AppConfig& config,
                                const ShardIndex& by_id, bool timer) {
  std::optional<uint64_t> id;
  uint64_t shard_number = 0;
  if (!input["shard"].error()) {
    if (input["shard"].get(shard_number) || shard_number >= kMaxShards) {
      return Invalid("invalid shard id");
    }
    id = shard_number;
  }
  std::optional<std::string> market;
  if (!input["market"].error()) {
    auto text = String(input, "market");
    if (!text.ok()) return text.status();
    market = std::move(*text);
  }
  const ShardConfig* selected = nullptr;
  if (id) {
    selected = by_id[*id];
  } else if (market) {
    for (const ShardConfig& shard : config.shards) {
      if (shard.market.id.symbol != *market) continue;
      if (selected) return Invalid("market routes to multiple shards");
      selected = &shard;
    }
  } else if (config.shards.size() == 1) {
    selected = &config.shards.front();
  }
  if (!selected) return Invalid("record has no unique target shard");
  if (market && selected->market.id.symbol != *market) {
    return Invalid("record market and shard disagree");
  }
  if (!timer && market && market->empty()) return Invalid("empty market");
  return selected->id;
}

}  // namespace

absl::Status ReadReplayFile(const std::string& path, const AppConfig& config,
                            const ReplaySink& sink) {
  simdjson::padded_string json;
  if (simdjson::padded_string::load(path).get(json)) {
    return absl::NotFoundError("cannot read replay file: " + path);
  }
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root)) return Invalid("invalid JSON");
  auto version = Unsigned(root, "schema_version");
  if (!version.ok() || (*version != 1 && *version != 2)) {
    return Invalid("unsupported schema_version");
  }
  simdjson::dom::array inputs;
  if (root["inputs"].get(inputs)) return Invalid("inputs array missing");
  ShardIndex by_id{};
  for (const ShardConfig& shard : config.shards) {
    by_id[shard.id.value] = &shard;
  }
  std::optional<InputTime> previous;
  std::map<uint16_t, uint64_t> epochs;
  for (auto input : inputs) {
    auto at = Signed(input, "at_us");
    auto ordinal = Unsigned(input, "ordinal");
    auto kind = String(input, "kind");
    if (!at.ok()) return at.status();
    if (!ordinal.ok()) return ordinal.status();
    if (!kind.ok()) return kind.status();
    if (*at < 0 || (previous &&
        (*at < previous->at_us ||
         (*at == previous->at_us && *ordinal <= previous->ordinal)))) {
      return Invalid("input times are not strictly ordered");
    }
    previous = InputTime{*at, *ordinal};
    const bool timer = *kind == "timer";
    if (timer && input["shard"].error() && input["market"].error()) {
      for (const ShardConfig& shard : config.shards) {
        ReplayRecord tick{shard.id, *previous, TimerTick{}};
        if (auto status = sink(std::move(tick)); !status.ok()) return status;
      }
      continue;
    }
    auto target = Target(input, config, by_id, timer);
    if (!target.ok()) return target.status();
    const MarketId market = by_id[target->value]->market.id;
    ReplayRecord record;
    record.target = *target;
    record.time = *previous;
    if (timer) {
      record.body = TimerTick{};
    } else if (*kind == "subscribe") {
      auto epoch = Unsigned(input, "connection_id");
      if (!epoch.ok()) return epoch.status();
      epochs[target->value] = *epoch;
      record.body = MarketInput(FeedConnected{*epoch});
    } else if (*kind == "disconnect") {
      auto epoch = Unsigned(input, "connection_id");
      if (!epoch.ok()) return epoch.status();
      std::string reason = "replay disconnect";
      if (!input["reason"].error()) {
        auto value = String(input, "reason");
        if (!value.ok()) return value.status();
        reason = std::move(*value);
      }
      record.body = MarketInput(FeedDisconnected{*epoch, std::move(reason)});
    } else if (*kind == "snapshot" || *kind == "diff") {
      auto epoch = Unsigned(input, "connection_id");
      auto bids = Levels(input, "bids");
      auto asks = Levels(input, "asks");
      if (!epoch.ok()) return epoch.status();
      if (!bids.ok()) return bids.status();
      if (!asks.ok()) return asks.status();
      if (*kind == "snapshot") {
        auto sequence = Unsigned(input, "last_sequence");
        if (!sequence.ok()) return sequence.status();
        record.body = MarketInput(BookSnapshot{market, *epoch, *sequence,
                                               std::move(*bids), std::move(*asks)});
      } else {
        auto first = Unsigned(input, "first_sequence");
        auto last = Unsigned(input, "last_sequence");
        if (!first.ok()) return first.status();
        if (!last.ok()) return last.status();
        record.body = MarketInput(BookDiff{market, *epoch, *first, *last,
                                           std::move(*bids), std::move(*asks)});
      }
    } else if (*kind == "public_trade") {
      auto price = Unsigned(input, "price_ticks");
      auto quantity = Unsigned(input, "quantity_lots");
      auto side = String(input, "side");
      if (!price.ok()) return price.status();
      if (!quantity.ok()) return quantity.status();
      if (!side.ok() || (*side != "buy" && *side != "sell")) {
        return Invalid("invalid public trade side");
      }
      uint64_t trade_id = *ordinal;
      if (!input["trade_id"].error()) {
        auto value = Unsigned(input, "trade_id");
        if (!value.ok()) return value.status();
        trade_id = *value;
      }
      uint64_t epoch = epochs[target->value];
      if (!input["connection_id"].error()) {
        auto value = Unsigned(input, "connection_id");
        if (!value.ok()) return value.status();
        epoch = *value;
      }
      record.body = MarketInput(PublicTrade{
          market, epoch, trade_id, *price, *quantity,
          *side == "buy" ? Side::Buy : Side::Sell});
    } else {
      return Invalid("unknown input kind");
    }
    if (auto status = sink(std::move(record)); !status.ok()) return status;
  }
  return absl::OkStatus();
}

}  // namespace hquant::v1
