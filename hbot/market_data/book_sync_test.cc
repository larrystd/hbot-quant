#include "hbot/market_data/book_sync.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "hbot/market_data/book_view.h"
#include "simdjson.h"
#include "tests/fixtures/fixture_loader.h"

namespace {

using hbot::BookApplyReason;
using hbot::BookSyncState;

void Require(bool value, const std::string& message) {
  if (!value) {
    std::cerr << message << '\n';
    std::exit(1);
  }
}

std::string String(simdjson::dom::element value, const char* key) {
  std::string_view text;
  Require(!value[key].get(text), std::string("missing string ") + key);
  return std::string(text);
}

uint64_t U64(simdjson::dom::element value, const char* key) {
  uint64_t number = 0;
  Require(!value[key].get(number), std::string("missing integer ") + key);
  return number;
}

std::vector<hbot::BookLevel> Levels(simdjson::dom::element value,
                                    const char* key) {
  simdjson::dom::array rows;
  Require(!value[key].get(rows), std::string("missing levels ") + key);
  std::vector<hbot::BookLevel> result;
  for (auto row : rows) {
    simdjson::dom::array pair;
    Require(!row.get(pair) && pair.size() == 2, "invalid level pair");
    int64_t price = 0;
    uint64_t quantity = 0;
    Require(!pair.at(0).get(price) && !pair.at(1).get(quantity),
            "invalid level number");
    result.push_back({hbot::PriceTicks{price}, hbot::QuantityLots{quantity}});
  }
  return result;
}

std::optional<hbot::BookLevel> OptionalLevel(simdjson::dom::element value) {
  if (value.is_null()) return std::nullopt;
  simdjson::dom::array pair;
  Require(!value.get(pair) && pair.size() == 2, "invalid BBO level");
  int64_t price = 0;
  uint64_t quantity = 0;
  Require(!pair.at(0).get(price) && !pair.at(1).get(quantity),
          "invalid BBO number");
  return hbot::BookLevel{{price}, {quantity}};
}

bool Same(const hbot::BookLevel& left, const hbot::BookLevel& right) {
  return left.price_ticks == right.price_ticks &&
         left.quantity_lots == right.quantity_lots;
}

bool Same(const std::optional<hbot::BookLevel>& left,
          const std::optional<hbot::BookLevel>& right) {
  return (!left && !right) || (left && right && Same(*left, *right));
}

std::string StateName(BookSyncState state) {
  switch (state) {
    case BookSyncState::Subscribing:
      return "Subscribing";
    case BookSyncState::Buffering:
      return "Buffering";
    case BookSyncState::Replaying:
      return "Replaying";
    case BookSyncState::Live:
      return "Live";
    case BookSyncState::Stale:
      return "Stale";
    case BookSyncState::Resyncing:
      return "Resyncing";
  }
  return "unknown";
}

std::string ReasonName(BookApplyReason reason) {
  switch (reason) {
    case BookApplyReason::None:
      return "None";
    case BookApplyReason::OldDiff:
      return "OldDiff";
    case BookApplyReason::Gap:
      return "Gap";
    case BookApplyReason::CrossedBook:
      return "CrossedBook";
    case BookApplyReason::InvalidScale:
      return "InvalidScale";
    case BookApplyReason::InvalidMessage:
      return "InvalidMessage";
    case BookApplyReason::BufferOverflow:
      return "BufferOverflow";
    case BookApplyReason::Disconnected:
      return "Disconnected";
    case BookApplyReason::Expired:
      return "Expired";
  }
  return "unknown";
}

hbot::EventTime TimeAt(int64_t at_us) {
  hbot::EventTime time;
  time.receive_mono = hbot::MonoTime(std::chrono::microseconds(at_us));
  time.receive_utc = hbot::UtcTime(std::chrono::microseconds(at_us));
  return time;
}

void CheckOutput(const hbot::BookApplyResult& actual,
                 const hbot::BookView& view, simdjson::dom::element expected,
                 const std::string& where) {
  Require(StateName(actual.state) == String(expected, "state"),
          where + " state");
  Require(ReasonName(actual.reason) == String(expected, "reason"),
          where + " reason");
  bool applied = false;
  Require(!expected["applied"].get(applied) && actual.applied == applied,
          where + " applied");
  simdjson::dom::element sequence;
  Require(!expected["last_sequence"].get(sequence), where + " sequence field");
  if (sequence.is_null()) {
    Require(!actual.last_sequence, where + " null sequence");
  } else {
    uint64_t number = 0;
    Require(!sequence.get(number) && actual.last_sequence == number,
            where + " sequence");
  }
  Require(view.State() == actual.state &&
              view.LastSequence() == actual.last_sequence,
          where + " view metadata");
  simdjson::dom::element bbo;
  Require(!expected["bbo"].get(bbo), where + " BBO");
  simdjson::dom::element bid, ask;
  Require(!bbo["bid"].get(bid) && !bbo["ask"].get(ask), where + " BBO sides");
  Require(Same(view.BestBid(), OptionalLevel(bid)), where + " best bid");
  Require(Same(view.BestAsk(), OptionalLevel(ask)), where + " best ask");
  for (const auto [side, key] : {std::pair{hbot::Side::Buy, "top_bids"},
                                 std::pair{hbot::Side::Sell, "top_asks"}}) {
    const auto levels = Levels(expected, key);
    std::array<hbot::BookLevel, 2> top{};
    const size_t count = view.CopyTopN(side, top);
    Require(count == levels.size(), where + " depth count " + key);
    for (size_t index = 0; index < count; ++index) {
      Require(Same(top[index], levels[index]), where + " depth level " + key);
    }
  }
}

void Replay(const std::filesystem::path& path) {
  auto fixture = hbot::fixtures::LoadFixture(path.string());
  Require(fixture.ok(), path.string() + " fixture load");
  Require(fixture->family == "order_book", path.string() + " family");
  simdjson::dom::parser setup_parser;
  simdjson::dom::element setup;
  Require(!setup_parser.parse(fixture->setup_json).get(setup), "setup parse");
  const std::string market_name = String(setup, "market");
  const uint64_t scale_version = U64(setup, "scale_version");
  uint64_t stale_after = 5'000'000;
  if (!setup["stale_after_us"].error())
    stale_after = U64(setup, "stale_after_us");
  hbot::MarketId market{hbot::VenueId("binance"), hbot::InstrumentKind::Spot,
                        market_name};
  hbot::BookSync sync(market, scale_version, 8192, 1024, stale_after);

  for (size_t index = 0; index < fixture->steps.size(); ++index) {
    const auto& step = fixture->steps[index];
    simdjson::dom::parser event_parser;
    simdjson::dom::element event;
    Require(!event_parser.parse(step.event_json).get(event), "event parse");
    const std::string kind = String(event, "kind");
    hbot::BookApplyResult result;
    if (kind == "subscribe") {
      result = sync.Subscribe(U64(event, "stream_epoch"));
    } else if (kind == "snapshot") {
      hbot::BookSnapshot snapshot;
      snapshot.market = market;
      snapshot.scale_version = scale_version;
      snapshot.stream_epoch = U64(event, "stream_epoch");
      snapshot.last_sequence = U64(event, "last_sequence");
      snapshot.bids = Levels(event, "bids");
      snapshot.asks = Levels(event, "asks");
      snapshot.time = TimeAt(step.stamp.at_us);
      result = sync.OnSnapshot(snapshot);
    } else if (kind == "diff") {
      hbot::BookDiff diff;
      diff.market = market;
      diff.scale_version = scale_version;
      diff.stream_epoch = U64(event, "stream_epoch");
      diff.first_sequence = U64(event, "first_sequence");
      diff.last_sequence = U64(event, "last_sequence");
      diff.bids = Levels(event, "bids");
      diff.asks = Levels(event, "asks");
      diff.time = TimeAt(step.stamp.at_us);
      result = sync.OnDiff(diff);
    } else if (kind == "timer") {
      result = sync.OnTimer(static_cast<uint64_t>(step.stamp.at_us));
    } else if (kind == "disconnect") {
      result = sync.OnDisconnect();
    } else if (kind == "invalid_raw_level") {
      result = sync.OnInvalidScale();
    } else {
      Require(false, "unknown event kind");
    }
    simdjson::dom::parser output_parser;
    simdjson::dom::element output;
    Require(!output_parser.parse(step.output_json).get(output), "output parse");
    CheckOutput(result, sync.View(), output,
                fixture->case_id + " step " + std::to_string(index));
  }
}

void CheckSyncEdges() {
  hbot::MarketId market{hbot::VenueId("binance"), hbot::InstrumentKind::Spot,
                        "BTC-USDT"};
  hbot::BookSync sync(market, 1, 16, 2, 100);
  Require(sync.Subscribe(1).state == BookSyncState::Buffering, "subscribe");
  hbot::BookSnapshot snapshot;
  snapshot.market = market;
  snapshot.scale_version = 1;
  snapshot.stream_epoch = 1;
  snapshot.last_sequence = 10;
  snapshot.bids = {{{99}, {1}}};
  snapshot.asks = {{{101}, {1}}};
  snapshot.time = TimeAt(10);
  Require(sync.OnSnapshot(snapshot).state == BookSyncState::Replaying,
          "snapshot awaits first bridge");
  Require(!sync.View().BestBid(), "replaying book is hidden");
  hbot::BookDiff bridge;
  bridge.market = market;
  bridge.scale_version = 1;
  bridge.stream_epoch = 1;
  bridge.first_sequence = 9;
  bridge.last_sequence = 11;
  bridge.time = TimeAt(11);
  Require(sync.OnDiff(bridge).state == BookSyncState::Live,
          "overlapping bridge goes live");
  Require(sync.View().BestBid().has_value(), "live book visible");
  Require(sync.OnDiff(bridge).reason == BookApplyReason::OldDiff,
          "old bridge ignored");
  Require(sync.OnTimer(112).state == BookSyncState::Stale,
          "silence marks stale");
  Require(sync.OnTimer(212).state == BookSyncState::Resyncing,
          "longer silence resynchronizes");

  sync.Subscribe(2);
  bridge.stream_epoch = 2;
  bridge.first_sequence = 1;
  bridge.last_sequence = 1;
  sync.OnDiff(bridge);
  bridge.first_sequence = 2;
  bridge.last_sequence = 2;
  sync.OnDiff(bridge);
  bridge.first_sequence = 3;
  bridge.last_sequence = 3;
  Require(sync.OnDiff(bridge).reason == BookApplyReason::BufferOverflow,
          "bounded diff buffer");
}

}  // namespace

int main() {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  Require(srcdir && workspace, "Bazel runfiles unavailable");
  const auto directory =
      std::filesystem::path(srcdir) / workspace / "tests/fixtures/order_book";
  std::vector<std::filesystem::path> paths;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.is_regular_file() && entry.path().extension() == ".json") {
      paths.push_back(entry.path());
    }
  }
  std::sort(paths.begin(), paths.end());
  Require(paths.size() >= 8, "missing order book fixtures");
  for (const auto& path : paths) Replay(path);
  CheckSyncEdges();
  return 0;
}
