#include "market/market_data_stream.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "apps/bench_feed.h"
#include "base/error.h"
#include "base/net.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/beast/http.hpp"
#include "boost/beast/websocket.hpp"
#include "gtest/gtest.h"
#include "hquant/test/fixture_loader.h"
#include "market/order_book.h"
#include "simdjson.h"

namespace hquant::binance_spot {
namespace {

Decimal ParserD(const char* text) { return *Decimal::Parse(text); }
MarketId ParserMarket() {
  return MarketId{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
}
TickLotSize ParserScale() {
  return TickLotSize{ParserD("0.01"), ParserD("0.001"), 1};
}

TEST(DepthParserTest, ParsesBenchFeedSnapshotDiffAndTrade) {
  const auto scale = ParserScale();
  DepthParser parser(ParserMarket(), scale);
  const std::vector<BookLevel> bids{{PriceTicks{9999}, QuantityLots{100}}};
  const std::vector<BookLevel> asks{{PriceTicks{10001}, QuantityLots{100}}};
  auto snapshot = parser.ParseSnapshot(
      BenchSnapshotBody(10, bids, asks, scale.price_per_tick,
                        scale.amount_per_lot),
      1, {});
  ASSERT_TRUE(snapshot.ok()) << snapshot.status();
  ASSERT_EQ(snapshot->bids.size(), 1);
  EXPECT_EQ(snapshot->bids[0].price_ticks.value, 9999);
  EXPECT_EQ(snapshot->bids[0].quantity_lots.value, 100);

  auto diff = parser.ParseDiff(
      BenchDepthFrame("BTCUSDT", 10, 11, {}, {}, scale.price_per_tick,
                      scale.amount_per_lot),
      1, {});
  ASSERT_TRUE(diff.ok()) << diff.status();
  EXPECT_EQ(diff->first_sequence, 10);
  EXPECT_EQ(diff->last_sequence, 11);

  auto trade = parser.ParseTrade(
      BenchTradeFrame("BTCUSDT", 1, PriceTicks{10100}, QuantityLots{10},
                      Side::Buy, scale.price_per_tick, scale.amount_per_lot),
      {});
  ASSERT_TRUE(trade.ok()) << trade.status();
  EXPECT_EQ(trade->price_ticks.value, 10100);
  EXPECT_EQ(trade->quantity_lots.value, 10);
  EXPECT_EQ(trade->side, Side::Buy);
}

TEST(DepthParserTest, SnapshotAndOverlappingDiffReplayThroughBookSync) {
  DepthParser parser(ParserMarket(), ParserScale());
  OrderBookSync sync(ParserMarket(), 1);
  EXPECT_EQ(sync.Subscribe(1).state, BookSyncState::WaitingSnapshot);
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
  EXPECT_EQ(sync.OnDiff(*diff).state, BookSyncState::WaitingSnapshot);

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
  DepthParser parser(ParserMarket(), ParserScale());
  auto invalid_scale = parser.ParseDiff(R"({"e":"depthUpdate","s":"BTCUSDT",
    "U":1,"u":1,"b":[["100.005","1.000"]],"a":[]})",
                                        1, {});
  EXPECT_EQ(CodeOf(invalid_scale.status()), ErrorCode::kFeedTickSizeMismatch);
  EXPECT_FALSE(parser
                   .ParseDiff(R"({"e":"depthUpdate","s":"BTCUSDT",
    "U":1,"u":1,"b":[["100.00","0.0001"]],"a":[]})",
                              1, {})
                   .ok());
  auto wrong_market = parser.ParseDiff(R"({"e":"depthUpdate","s":"ETHUSDT",
    "U":1,"u":1,"b":[],"a":[]})",
                                       1, {});
  EXPECT_EQ(CodeOf(wrong_market.status()), ErrorCode::kFeedWrongMarket);
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

TEST(DepthParserTest, WrongMarketResynchronizesBook) {
  OrderBookSync sync(ParserMarket(), 1);
  sync.Subscribe(1);
  BookDiff wrong;
  wrong.market =
      MarketId{ExchangeId("binance"), InstrumentKind::Spot, "ETHUSDT"};
  wrong.tick_lot_version = 1;
  wrong.connection_id = 1;
  wrong.first_sequence = 1;
  wrong.last_sequence = 1;
  const auto result = sync.OnDiff(wrong);
  EXPECT_EQ(result.state, BookSyncState::Resyncing);
  EXPECT_EQ(result.reason, ErrorCode::kFeedWrongMarket);
}

TEST(DepthParserTest, PublicTradeDirectionAndUnits) {
  DepthParser parser(ParserMarket(), ParserScale());
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

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using Tcp = asio::ip::tcp;

Decimal D(const char* text) { return *Decimal::Parse(text); }
MarketId Market() {
  return MarketId{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
}

class TestClock final : public Clock {
 public:
  UtcTime UtcNow() const override {
    return UtcTime(std::chrono::microseconds(1000));
  }
  MonoTime MonoNow() const override {
    return MonoTime(std::chrono::microseconds(1000));
  }
};

// 测试用的行情接收方：把数据应用到一个订单簿，并把结果和错误交给测试。
class RecordingReceiver final : public MarketDataReceiver {
 public:
  explicit RecordingReceiver(OrderBookSync& book) : book_(book) {}

  BookApplyResult OnConnect(uint64_t connection_id) override {
    return Record(book_.Subscribe(connection_id));
  }
  BookApplyResult OnSnapshot(const BookSnapshot& snapshot) override {
    return Record(book_.OnSnapshot(snapshot));
  }
  BookApplyResult OnDiff(const BookDiff& diff) override {
    return Record(book_.OnDiff(diff));
  }
  BookApplyResult OnDisconnect() override {
    return Record(book_.OnDisconnect());
  }
  void OnPublicTrade(const PublicTrade&) override {}
  void OnStreamError(const absl::Status& status) override {
    if (on_error) on_error(status);
  }

  std::function<void(const BookApplyResult&)> on_result;
  std::function<void(const absl::Status&)> on_error;

 private:
  BookApplyResult Record(BookApplyResult result) {
    if (on_result) on_result(result);
    return result;
  }

  OrderBookSync& book_;
};

void RunLocalCycle(uint64_t second_first, bool retry_snapshot,
                   absl::StatusCode expected_status, unsigned http_status = 200,
                   bool delay_http = false,
                   std::optional<ErrorCode> expected_code = std::nullopt) {
  asio::io_context server_io;
  Tcp::acceptor http_acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  Tcp::acceptor ws_acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const std::string http_port =
      std::to_string(http_acceptor.local_endpoint().port());
  const std::string ws_port =
      std::to_string(ws_acceptor.local_endpoint().port());
  std::string target_seen;
  std::thread http_server([&] {
    Tcp::socket socket(server_io);
    http_acceptor.accept(socket);
    const int requests = retry_snapshot ? 2 : 1;
    for (int i = 0; i < requests; ++i) {
      beast::flat_buffer buffer;
      http::request<http::string_body> request;
      http::read(socket, buffer, request);
      target_seen = std::string(request.target());
      if (delay_http) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return;
      }
      http::response<http::string_body> response{
          static_cast<http::status>(http_status), 11};
      response.keep_alive(i + 1 < requests);
      response.body() =
          std::string(R"({"lastUpdateId":)") +
          (retry_snapshot && i == 0 ? "98" : "100") +
          R"(,"bids":[["99.90","1.000"]],"asks":[["100.10","1.000"]]})";
      response.prepare_payload();
      http::write(socket, response);
    }
  });
  std::thread ws_server([&] {
    Tcp::socket socket(server_io);
    ws_acceptor.accept(socket);
    websocket::stream<Tcp::socket> ws(std::move(socket));
    ws.accept();
    ws.write(asio::buffer(std::string(R"({"e":"depthUpdate","s":"BTCUSDT",
      "U":99,"u":101,"b":[["100.00","2.000"]],"a":[]})")));
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    const std::string next = std::string(R"({"e":"depthUpdate","s":"BTCUSDT",
      "U":)") + std::to_string(second_first) +
                             R"(,"u":103,"b":[["100.01","1.000"]],"a":[]})";
    ws.write(asio::buffer(next));
  });

  asio::io_context io;
  HttpClient http_client(io, "127.0.0.1", http_port);
  WebSocketClient websocket(io, "127.0.0.1", ws_port, "/ws/btcusdt@depth");
  TickLotSize scale{D("0.01"), D("0.001"), 1};
  OrderBookSync book(Market(), 1);
  TestClock clock;
  StreamConfig config;
  config.symbol = "BTCUSDT";
  config.snapshot_timeout =
      delay_http ? std::chrono::milliseconds(30) : std::chrono::seconds(2);
  config.read_timeout = std::chrono::seconds(2);
  std::vector<BookApplyResult> results;
  RecordingReceiver receiver(book);
  receiver.on_result = [&](const BookApplyResult& result) {
    results.push_back(result);
  };
  MarketDataStream stream(config, DepthParser(Market(), scale), http_client,
                          websocket, receiver, clock);
  absl::Status outcome;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> { outcome = co_await stream.RunCycle(2); },
      asio::detached);
  io.run();
  http_server.join();
  ws_server.join();

  EXPECT_EQ(outcome.code(), expected_status) << outcome;
  if (expected_code) EXPECT_EQ(CodeOf(outcome), *expected_code);
  EXPECT_EQ(stream.StreamEpoch(), 1);
  EXPECT_EQ(target_seen, "/api/v3/depth?symbol=BTCUSDT&limit=1000");
  ASSERT_GE(results.size(), 3);
  if (expected_status == absl::StatusCode::kOk) {
    EXPECT_EQ(book.View().State(), BookSyncState::Live);
    EXPECT_EQ(book.View().LastSequence(), 103);
    ASSERT_TRUE(book.View().BestBid());
    EXPECT_EQ(book.View().BestBid()->price_ticks.value, 10001);
  } else if (expected_status == absl::StatusCode::kAborted) {
    EXPECT_EQ(book.View().State(), BookSyncState::Resyncing);
    bool saw_gap = false;
    for (const auto& result : results) {
      if (result.reason == ErrorCode::kBookSequenceGap) saw_gap = true;
    }
    EXPECT_TRUE(saw_gap);
  }
}

TEST(MarketDataStreamTest, LocalHttpSnapshotRetryAndWebSocketReplay) {
  RunLocalCycle(102, true, absl::StatusCode::kOk);
}

TEST(MarketDataStreamTest, GapStopsCycleAndRequestsResync) {
  RunLocalCycle(103, false, absl::StatusCode::kAborted);
}

TEST(MarketDataStreamTest, HttpThrottleStopsCycle) {
  RunLocalCycle(102, false, absl::StatusCode::kResourceExhausted, 429, false,
                ErrorCode::kExchangeRateLimited);
}

TEST(MarketDataStreamTest, IpBanHaltsCycle) {
  RunLocalCycle(102, false, absl::StatusCode::kResourceExhausted, 418, false,
                ErrorCode::kExchangeIpBanned);
}

TEST(MarketDataStreamTest, SnapshotDeadlineStopsCycle) {
  RunLocalCycle(102, false, absl::StatusCode::kDeadlineExceeded, 200, true);
}

TEST(MarketDataStreamTest, InvalidConfigStopsRun) {
  asio::io_context io;
  HttpClient http_client(io, "127.0.0.1", "1");
  WebSocketClient websocket(io, "127.0.0.1", "1", "/ws");
  OrderBookSync book(Market(), 1);
  TestClock clock;
  StreamConfig config;
  config.symbol = "ETHUSDT";
  RecordingReceiver receiver(book);
  MarketDataStream stream(config, DepthParser(Market(), ParserScale()),
                          http_client, websocket, receiver, clock);
  bool guard_fired = false;
  bool completed = false;
  asio::steady_timer guard(io);
  guard.expires_after(std::chrono::milliseconds(100));
  guard.async_wait([&](const boost::system::error_code& ec) {
    if (!ec) {
      guard_fired = true;
      stream.Stop();
    }
  });
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        co_await stream.Run();
        completed = true;
        guard.cancel();
      },
      asio::detached);
  io.run();
  EXPECT_TRUE(completed);
  EXPECT_FALSE(guard_fired);
  EXPECT_EQ(stream.StreamEpoch(), 0);
}

TEST(MarketDataStreamTest, InvalidMessageReconnectsAndRecovers) {
  asio::io_context server_io;
  Tcp::acceptor http_acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  Tcp::acceptor ws_acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  http_acceptor.non_blocking(true);
  ws_acceptor.non_blocking(true);
  const std::string http_port =
      std::to_string(http_acceptor.local_endpoint().port());
  const std::string ws_port =
      std::to_string(ws_acceptor.local_endpoint().port());
  std::atomic<int> connections = 0;
  auto accept_until = [&](Tcp::acceptor& acceptor, Tcp::socket& socket) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    boost::system::error_code ec;
    while (std::chrono::steady_clock::now() < deadline) {
      acceptor.accept(socket, ec);
      if (!ec) return true;
      if (ec != asio::error::would_block && ec != asio::error::try_again)
        return false;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
  };
  std::thread ws_server([&] {
    for (int attempt = 0; attempt < 2; ++attempt) {
      Tcp::socket socket(server_io);
      if (!accept_until(ws_acceptor, socket)) return;
      ++connections;
      websocket::stream<Tcp::socket> ws(std::move(socket));
      ws.accept();
      if (attempt == 0) {
        ws.write(asio::buffer(std::string("{bad JSON")));
      } else {
        ws.write(asio::buffer(std::string(R"({"e":"depthUpdate",
          "s":"BTCUSDT","U":99,"u":101,"b":[["100.00","2.000"]],"a":[]})")));
      }
    }
  });
  std::thread http_server([&] {
    Tcp::socket socket(server_io);
    if (!accept_until(http_acceptor, socket)) return;
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    http::response<http::string_body> response{http::status::ok, 11};
    response.body() = R"({"lastUpdateId":100,
      "bids":[["99.90","1.000"]],"asks":[["100.10","1.000"]]})";
    response.prepare_payload();
    http::write(socket, response);
  });

  asio::io_context io;
  HttpClient http_client(io, "127.0.0.1", http_port);
  WebSocketClient websocket_client(io, "127.0.0.1", ws_port, "/ws");
  OrderBookSync book(Market(), 1);
  TestClock clock;
  StreamConfig config;
  config.symbol = "BTCUSDT";
  config.reconnect_delay = std::chrono::milliseconds(1);
  config.max_reconnect_delay = std::chrono::milliseconds(10);
  std::vector<ErrorCode> errors;
  bool live = false;
  MarketDataStream* stream_ptr = nullptr;
  RecordingReceiver receiver(book);
  receiver.on_result = [&](const BookApplyResult& result) {
    if (result.state == BookSyncState::Live) {
      live = true;
      stream_ptr->Stop();
    }
  };
  receiver.on_error = [&](const absl::Status& status) {
    errors.push_back(CodeOf(status));
  };
  MarketDataStream stream(config, DepthParser(Market(), ParserScale()),
                          http_client, websocket_client, receiver, clock);
  stream_ptr = &stream;
  asio::steady_timer guard(io);
  guard.expires_after(std::chrono::seconds(1));
  guard.async_wait([&](const boost::system::error_code& ec) {
    if (!ec) stream.Stop();
  });
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        co_await stream.Run();
        guard.cancel();
      },
      asio::detached);
  io.run();
  ws_server.join();
  http_server.join();
  EXPECT_TRUE(live);
  EXPECT_EQ(connections, 2);
  EXPECT_GE(stream.StreamEpoch(), 2);
  EXPECT_NE(
      std::find(errors.begin(), errors.end(), ErrorCode::kFeedMessageInvalid),
      errors.end());
}

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

void ExpectBbo(simdjson::dom::element expected, const OrderBookView& view) {
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
  const auto directory = std::filesystem::path(srcdir) / workspace /
                         "hquant/test/fixtures/order_book";
  for (const char* name : {"pre_snapshot_buffer", "overlapping_first_diff",
                           "continuous_replace_and_delete"}) {
    auto fixture = fixtures::LoadFixture(
        (directory / (std::string(name) + ".json")).string());
    ASSERT_TRUE(fixture.ok()) << fixture.status();
    SCOPED_TRACE(name);
    MarketId market{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
    DepthParser parser(market, TickLotSize{*Decimal::Parse("0.01"),
                                           *Decimal::Parse("0.001"), 1});
    OrderBookSync sync(market, 1);
    for (const auto& step : fixture->steps) {
      SCOPED_TRACE(step.stamp.at_us);
      simdjson::dom::parser input_parser, output_parser;
      auto event = input_parser.parse(step.event_json).value();
      auto expected = output_parser.parse(step.output_json).value();
      std::string_view kind = event["kind"];
      BookApplyResult result;
      if (kind == "subscribe") {
        result = sync.Subscribe(uint64_t(event["connection_id"]));
      } else if (kind == "snapshot") {
        std::string json = "{\"lastUpdateId\":" +
                           std::to_string(uint64_t(event["last_sequence"])) +
                           ",\"bids\":" + RawLevels(event, "bids") +
                           ",\"asks\":" + RawLevels(event, "asks") + "}";
        auto snapshot =
            parser.ParseSnapshot(json, uint64_t(event["connection_id"]), {});
        ASSERT_TRUE(snapshot.ok()) << snapshot.status();
        result = sync.OnSnapshot(*snapshot);
      } else if (kind == "diff") {
        std::string json =
            "{\"e\":\"depthUpdate\",\"s\":\"BTCUSDT\",\"U\":" +
            std::to_string(uint64_t(event["first_sequence"])) +
            ",\"u\":" + std::to_string(uint64_t(event["last_sequence"])) +
            ",\"b\":" + RawLevels(event, "bids") +
            ",\"a\":" + RawLevels(event, "asks") + "}";
        auto diff =
            parser.ParseDiff(json, uint64_t(event["connection_id"]), {});
        ASSERT_TRUE(diff.ok()) << diff.status();
        result = sync.OnDiff(*diff);
      } else
        FAIL() << "unexpected F1 event";

      std::string_view state = expected["state"];
      EXPECT_EQ(result.state == BookSyncState::WaitingSnapshot
                    ? "WaitingSnapshot"
                    : "Live",
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
}  // namespace hquant::binance_spot
