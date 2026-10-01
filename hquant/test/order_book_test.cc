#include "market/order_book.h"

#include <array>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <vector>

namespace {

hquant::BookLevel BookLevelForTest(int64_t price, uint64_t quantity) {
  return {{price}, {quantity}};
}

void BookRequire(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << '\n';
    std::exit(1);
  }
}

bool BookSame(const hquant::BookLevel& left, const hquant::BookLevel& right) {
  return left.price_ticks == right.price_ticks &&
         left.quantity_lots == right.quantity_lots;
}

bool BookSame(const std::optional<hquant::BookLevel>& left,
              const hquant::BookLevel& right) {
  return left && BookSame(*left, right);
}

}  // namespace

int RunOrderBookChecks() {
  hquant::OrderBook book(8);
  const std::vector bids{BookLevelForTest(100, 10), BookLevelForTest(90, 20)};
  const std::vector asks{BookLevelForTest(110, 5), BookLevelForTest(200, 6)};
  book.ApplySnapshot(bids, asks);
  book.SetMetadata(hquant::BookSyncState::Live, 1);
  BookRequire(BookSame(book.BestBid(), BookLevelForTest(100, 10)),
              "dense best bid");
  BookRequire(BookSame(book.BestAsk(), BookLevelForTest(110, 5)),
              "overflow best ask");
  std::array<hquant::BookLevel, 3> top{};
  BookRequire(book.CopyTopN(hquant::Side::Buy, top) == 2, "bid depth count");
  BookRequire(BookSame(top[0], BookLevelForTest(100, 10)) &&
                  BookSame(top[1], BookLevelForTest(90, 20)),
              "bid depth order");
  BookRequire(book.CopyTopN(hquant::Side::Sell, top) == 2, "ask depth count");
  BookRequire(BookSame(top[0], BookLevelForTest(110, 5)) &&
                  BookSame(top[1], BookLevelForTest(200, 6)),
              "ask depth order");

  const std::vector changed_bids{BookLevelForTest(100, 0),
                                 BookLevelForTest(95, 7)};
  const std::vector changed_asks{BookLevelForTest(110, 0),
                                 BookLevelForTest(105, 9)};
  book.ApplyDiff(changed_bids, changed_asks);
  BookRequire(BookSame(book.BestBid(), BookLevelForTest(95, 7)),
              "recentered best bid");
  BookRequire(BookSame(book.BestAsk(), BookLevelForTest(105, 9)),
              "recentered best ask");
  BookRequire(!book.IsCrossed(), "valid spread");

  const std::vector crossed{BookLevelForTest(106, 1)};
  book.ApplyDiff(crossed, {});
  BookRequire(book.IsCrossed(), "crossed spread detected");
  book.SetMetadata(hquant::BookSyncState::Resyncing, 2);
  BookRequire(!book.BestBid() && book.CopyTopN(hquant::Side::Buy, top) == 0,
              "unsafe book hidden");
  book.Reset();
  BookRequire(!book.BestAsk(), "reset clears book");
  return 0;
}

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string>
#include <string_view>

#include "hquant/test/fixture_loader.h"
#include "simdjson.h"

namespace {

using hquant::BookApplyOutcome;
using hquant::BookApplyResult;
using hquant::BookSyncState;
using hquant::ErrorCode;

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

std::vector<hquant::BookLevel> Levels(simdjson::dom::element value,
                                      const char* key) {
  simdjson::dom::array rows;
  Require(!value[key].get(rows), std::string("missing levels ") + key);
  std::vector<hquant::BookLevel> result;
  for (auto row : rows) {
    simdjson::dom::array pair;
    Require(!row.get(pair) && pair.size() == 2, "invalid level pair");
    int64_t price = 0;
    uint64_t quantity = 0;
    Require(!pair.at(0).get(price) && !pair.at(1).get(quantity),
            "invalid level number");
    result.push_back(
        {hquant::PriceTicks{price}, hquant::QuantityLots{quantity}});
  }
  return result;
}

std::optional<hquant::BookLevel> OptionalLevel(simdjson::dom::element value) {
  if (value.is_null()) return std::nullopt;
  simdjson::dom::array pair;
  Require(!value.get(pair) && pair.size() == 2, "invalid BBO level");
  int64_t price = 0;
  uint64_t quantity = 0;
  Require(!pair.at(0).get(price) && !pair.at(1).get(quantity),
          "invalid BBO number");
  return hquant::BookLevel{{price}, {quantity}};
}

bool Same(const hquant::BookLevel& left, const hquant::BookLevel& right) {
  return left.price_ticks == right.price_ticks &&
         left.quantity_lots == right.quantity_lots;
}

bool Same(const std::optional<hquant::BookLevel>& left,
          const std::optional<hquant::BookLevel>& right) {
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

std::string ReasonName(const BookApplyResult& result) {
  if (result.outcome == BookApplyOutcome::OldDiff) return "OldDiff";
  switch (result.reason) {
    case ErrorCode::kOk:
      return "None";
    case ErrorCode::kBookSequenceGap:
      return "Gap";
    case ErrorCode::kBookCrossed:
      return "CrossedBook";
    case ErrorCode::kFeedTickSizeMismatch:
      return "InvalidScale";
    case ErrorCode::kFeedMessageInvalid:
      return "InvalidMessage";
    case ErrorCode::kBookBufferOverflow:
      return "BufferOverflow";
    case ErrorCode::kFeedDisconnected:
      return "Disconnected";
    case ErrorCode::kBookStale:
      return "Expired";
    default:
      return "unknown";
  }
}

hquant::EventTime TimeAt(int64_t at_us) {
  hquant::EventTime time;
  time.receive_mono = hquant::MonoTime(std::chrono::microseconds(at_us));
  time.receive_utc = hquant::UtcTime(std::chrono::microseconds(at_us));
  return time;
}

void CheckOutput(const hquant::BookApplyResult& actual,
                 const hquant::BookView& view, simdjson::dom::element expected,
                 const std::string& where) {
  Require(StateName(actual.state) == String(expected, "state"),
          where + " state");
  Require(ReasonName(actual) == String(expected, "reason"), where + " reason");
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
  for (const auto [side, key] : {std::pair{hquant::Side::Buy, "top_bids"},
                                 std::pair{hquant::Side::Sell, "top_asks"}}) {
    const auto levels = Levels(expected, key);
    std::array<hquant::BookLevel, 2> top{};
    const size_t count = view.CopyTopN(side, top);
    Require(count == levels.size(), where + " depth count " + key);
    for (size_t index = 0; index < count; ++index) {
      Require(Same(top[index], levels[index]), where + " depth level " + key);
    }
  }
}

void Replay(const std::filesystem::path& path) {
  auto fixture = hquant::fixtures::LoadFixture(path.string());
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
  hquant::MarketId market{hquant::ExchangeId("binance"),
                          hquant::InstrumentKind::Spot, market_name};
  hquant::BookSync sync(market, scale_version, 8192, 1024, stale_after);

  for (size_t index = 0; index < fixture->steps.size(); ++index) {
    const auto& step = fixture->steps[index];
    simdjson::dom::parser event_parser;
    simdjson::dom::element event;
    Require(!event_parser.parse(step.event_json).get(event), "event parse");
    const std::string kind = String(event, "kind");
    hquant::BookApplyResult result;
    if (kind == "subscribe") {
      result = sync.Subscribe(U64(event, "stream_epoch"));
    } else if (kind == "snapshot") {
      hquant::BookSnapshot snapshot;
      snapshot.market = market;
      snapshot.scale_version = scale_version;
      snapshot.stream_epoch = U64(event, "stream_epoch");
      snapshot.last_sequence = U64(event, "last_sequence");
      snapshot.bids = Levels(event, "bids");
      snapshot.asks = Levels(event, "asks");
      snapshot.time = TimeAt(step.stamp.at_us);
      result = sync.OnSnapshot(snapshot);
    } else if (kind == "diff") {
      hquant::BookDiff diff;
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
  hquant::MarketId market{hquant::ExchangeId("binance"),
                          hquant::InstrumentKind::Spot, "BTC-USDT"};
  hquant::BookSync sync(market, 1, 16, 2, 100);
  Require(sync.Subscribe(1).state == BookSyncState::Buffering, "subscribe");
  hquant::BookSnapshot snapshot;
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
  hquant::BookDiff bridge;
  bridge.market = market;
  bridge.scale_version = 1;
  bridge.stream_epoch = 1;
  bridge.first_sequence = 9;
  bridge.last_sequence = 11;
  bridge.time = TimeAt(11);
  Require(sync.OnDiff(bridge).state == BookSyncState::Live,
          "overlapping bridge goes live");
  Require(sync.View().BestBid().has_value(), "live book visible");
  Require(sync.OnDiff(bridge).outcome == BookApplyOutcome::OldDiff,
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
  Require(sync.OnDiff(bridge).reason == ErrorCode::kBookBufferOverflow,
          "bounded diff buffer");
}

}  // namespace

int RunBookSyncChecks() {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  Require(srcdir && workspace, "Bazel runfiles unavailable");
  const auto directory = std::filesystem::path(srcdir) / workspace /
                         "hquant/test/fixtures/order_book";
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

int main() {
  if (RunOrderBookChecks() != 0) return 1;
  return RunBookSyncChecks();
}
