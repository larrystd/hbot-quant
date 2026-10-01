#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>

#include "gtest/gtest.h"
#include "hbot/connector/binance_spot/public/depth_parser.h"
#include "hbot/market_data/book_sync.h"
#include "hbot/market_data/book_view.h"
#include "simdjson.h"
#include "tests/fixtures/fixture_loader.h"

namespace hbot::binance_spot {
namespace {

std::string Fixed(uint64_t value, unsigned decimals) {
  std::string digits = std::to_string(value);
  if (digits.size() <= decimals)
    digits.insert(0, decimals + 1 - digits.size(), '0');
  digits.insert(digits.size() - decimals, 1, '.');
  return digits;
}

std::string RawLevels(simdjson::dom::element event, const char* key) {
  std::string result = "[";
  bool first = true;
  for (auto pair_element : simdjson::dom::array(event[key])) {
    auto pair = simdjson::dom::array(pair_element);
    if (!first) result += ",";
    first = false;
    result += "[\"" + Fixed(int64_t(pair.at(0)), 2) + "\",\"" +
              Fixed(uint64_t(pair.at(1)), 3) + "\"]";
  }
  return result + "]";
}

void ExpectBbo(simdjson::dom::element expected, const BookView& view) {
  auto bbo = expected["bbo"].value();
  for (const auto& [name, actual] :
       {std::pair<const char*, std::optional<BookLevel>>{"bid", view.BestBid()},
        {"ask", view.BestAsk()}}) {
    auto raw = bbo[name].value();
    if (raw.is_null()) {
      EXPECT_FALSE(actual);
    } else {
      ASSERT_TRUE(actual);
      auto pair = simdjson::dom::array(raw);
      EXPECT_EQ(actual->price_ticks.value, int64_t(pair.at(0)));
      EXPECT_EQ(actual->quantity_lots.value, uint64_t(pair.at(1)));
    }
  }
}

TEST(F1AdapterTest, ReplaysOrderBookFixturesThroughRawBinanceJson) {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  ASSERT_NE(srcdir, nullptr);
  ASSERT_NE(workspace, nullptr);
  const auto directory =
      std::filesystem::path(srcdir) / workspace / "tests/fixtures/order_book";
  for (const char* name : {"pre_snapshot_buffer", "overlapping_first_diff",
                           "continuous_replace_and_delete"}) {
    auto fixture = fixtures::LoadFixture(
        (directory / (std::string(name) + ".json")).string());
    ASSERT_TRUE(fixture.ok()) << fixture.status();
    SCOPED_TRACE(name);
    MarketId market{VenueId("binance"), InstrumentKind::Spot, "BTCUSDT"};
    DepthParser parser(market, BookScale{*Decimal::Parse("0.01"),
                                         *Decimal::Parse("0.001"), 1});
    BookSync sync(market, 1);
    for (const auto& step : fixture->steps) {
      SCOPED_TRACE(step.stamp.at_us);
      simdjson::dom::parser input_parser, output_parser;
      auto event = input_parser.parse(step.event_json).value();
      auto expected = output_parser.parse(step.output_json).value();
      std::string_view kind = event["kind"];
      BookApplyResult result;
      if (kind == "subscribe") {
        result = sync.Subscribe(uint64_t(event["stream_epoch"]));
      } else if (kind == "snapshot") {
        std::string json = "{\"lastUpdateId\":" +
                           std::to_string(uint64_t(event["last_sequence"])) +
                           ",\"bids\":" + RawLevels(event, "bids") +
                           ",\"asks\":" + RawLevels(event, "asks") + "}";
        auto snapshot =
            parser.ParseSnapshot(json, uint64_t(event["stream_epoch"]), {});
        ASSERT_TRUE(snapshot.ok()) << snapshot.status();
        result = sync.OnSnapshot(*snapshot);
      } else if (kind == "diff") {
        std::string json =
            "{\"e\":\"depthUpdate\",\"s\":\"BTCUSDT\",\"U\":" +
            std::to_string(uint64_t(event["first_sequence"])) +
            ",\"u\":" + std::to_string(uint64_t(event["last_sequence"])) +
            ",\"b\":" + RawLevels(event, "bids") +
            ",\"a\":" + RawLevels(event, "asks") + "}";
        auto diff = parser.ParseDiff(json, uint64_t(event["stream_epoch"]), {});
        ASSERT_TRUE(diff.ok()) << diff.status();
        result = sync.OnDiff(*diff);
      } else
        FAIL() << "unexpected F1 event";

      std::string_view state = expected["state"];
      EXPECT_EQ(result.state == BookSyncState::Buffering ? "Buffering" : "Live",
                state);
      auto sequence = expected["last_sequence"].value();
      if (sequence.is_null())
        EXPECT_FALSE(sync.View().LastSequence());
      else {
        ASSERT_TRUE(sync.View().LastSequence());
        EXPECT_EQ(*sync.View().LastSequence(), uint64_t(sequence));
      }
      ExpectBbo(expected, sync.View());
    }
  }
}

}  // namespace
}  // namespace hbot::binance_spot
