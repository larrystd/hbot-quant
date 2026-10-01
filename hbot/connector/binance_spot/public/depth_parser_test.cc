#include "hbot/connector/binance_spot/public/depth_parser.h"

#include <string>

#include "gtest/gtest.h"
#include "hbot/market_data/book_sync.h"
#include "hbot/market_data/book_view.h"

namespace hbot::binance_spot {
namespace {

Decimal D(const char* text) { return *Decimal::Parse(text); }
MarketId Market() {
  return MarketId{VenueId("binance"), InstrumentKind::Spot, "BTCUSDT"};
}
BookScale Scale() { return BookScale{D("0.01"), D("0.001"), 1}; }

TEST(DepthParserTest, SnapshotAndOverlappingDiffReplayThroughBookSync) {
  DepthParser parser(Market(), Scale());
  BookSync sync(Market(), 1);
  EXPECT_EQ(sync.Subscribe(1).state, BookSyncState::Buffering);
  auto diff = parser.ParseDiff(R"({"e":"depthUpdate","E":1700000000000,
    "s":"BTCUSDT","U":99,"u":103,"b":[["99.90","0"],
    ["100.00","2.000"]],"a":[["100.10","1.000"]]})",
                               1, {});
  ASSERT_TRUE(diff.ok()) << diff.status();
  EXPECT_EQ(diff->first_sequence, 99);
  EXPECT_EQ(diff->last_sequence, 103);
  ASSERT_EQ(diff->bids.size(), 2);
  EXPECT_EQ(diff->bids[0].price_ticks.value, 9990);
  EXPECT_EQ(diff->bids[0].quantity_lots.value, 0);
  EXPECT_EQ(diff->bids[1].quantity_lots.value, 2000);
  EXPECT_TRUE(diff->time.exchange_utc.has_value());
  EXPECT_EQ(sync.OnDiff(*diff).state, BookSyncState::Buffering);

  auto snapshot = parser.ParseSnapshot(R"({"lastUpdateId":100,
    "bids":[["99.90","1.000"],["99.80","1.000"]],
    "asks":[["100.10","1.000"],["100.20","1.000"]]})",
                                       1, {});
  ASSERT_TRUE(snapshot.ok()) << snapshot.status();
  EXPECT_EQ(snapshot->bids[0].price_ticks.value, 9990);
  EXPECT_EQ(snapshot->bids[0].quantity_lots.value, 1000);
  EXPECT_EQ(sync.OnSnapshot(*snapshot).state, BookSyncState::Live);
  ASSERT_TRUE(sync.View().BestBid());
  EXPECT_EQ(sync.View().BestBid()->price_ticks.value, 10000);
  EXPECT_EQ(sync.View().LastSequence(), 103);

  auto gap = parser.ParseDiff(R"({"stream":"btcusdt@depth","data":{
    "e":"depthUpdate","s":"BTCUSDT","U":105,"u":105,
    "b":[],"a":[]}})",
                              1, {});
  ASSERT_TRUE(gap.ok()) << gap.status();
  EXPECT_EQ(sync.OnDiff(*gap).state, BookSyncState::Resyncing);
}

TEST(DepthParserTest, RejectsInvalidRawScaleAndWrongSymbol) {
  DepthParser parser(Market(), Scale());
  EXPECT_FALSE(parser
                   .ParseDiff(R"({"e":"depthUpdate","s":"BTCUSDT",
    "U":1,"u":1,"b":[["100.005","1.000"]],"a":[]})",
                              1, {})
                   .ok());
  EXPECT_FALSE(parser
                   .ParseDiff(R"({"e":"depthUpdate","s":"BTCUSDT",
    "U":1,"u":1,"b":[["100.00","0.0001"]],"a":[]})",
                              1, {})
                   .ok());
  EXPECT_FALSE(parser
                   .ParseDiff(R"({"e":"depthUpdate","s":"ETHUSDT",
    "U":1,"u":1,"b":[],"a":[]})",
                              1, {})
                   .ok());
  EXPECT_FALSE(parser
                   .ParseDiff(R"({"e":"depthUpdate","s":"BTCUSDT",
    "U":2,"u":1,"b":[],"a":[]})",
                              1, {})
                   .ok());
  EXPECT_FALSE(parser
                   .ParseSnapshot(R"({"lastUpdateId":1,
    "bids":[["92233720368547758.08","1.000"]],"asks":[]})",
                                  1, {})
                   .ok());
  EXPECT_FALSE(parser
                   .ParseSnapshot(R"({"lastUpdateId":1,
    "bids":[["100.00","18446744073709551.616"]],"asks":[]})",
                                  1, {})
                   .ok());
}

TEST(DepthParserTest, PublicTradeDirectionAndUnits) {
  DepthParser parser(Market(), Scale());
  auto sell = parser.ParseTrade(R"({"e":"trade","E":1700000000000,
    "s":"BTCUSDT","t":12345,"p":"100.01","q":"0.001",
    "m":true})",
                                {});
  ASSERT_TRUE(sell.ok()) << sell.status();
  EXPECT_EQ(sell->price_ticks.value, 10001);
  EXPECT_EQ(sell->quantity_lots.value, 1);
  EXPECT_EQ(sell->side, Side::Sell);
  EXPECT_EQ(sell->public_trade_id, "12345");
  auto buy = parser.ParseTrade(R"({"e":"aggTrade","s":"BTCUSDT","a":56,
    "p":"100.02","q":"2.000","m":false})",
                               {});
  ASSERT_TRUE(buy.ok()) << buy.status();
  EXPECT_EQ(buy->side, Side::Buy);
  EXPECT_EQ(buy->quantity_lots.value, 2000);
}

}  // namespace
}  // namespace hbot::binance_spot
