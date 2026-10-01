#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "base/net.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/beast/http.hpp"
#include "boost/beast/websocket.hpp"
#include "gtest/gtest.h"
#include "market/binance_spot_feed.h"
#include "order/paper.h"
#include "order/risk.h"
#include "service/shard.h"
#include "strategy/simple_pmm.h"

namespace hquant {
namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using Tcp = asio::ip::tcp;

Decimal D(const char* text) { return *Decimal::Parse(text); }

class FixedClock final : public Clock {
 public:
  UtcTime UtcNow() const override {
    return UtcTime(std::chrono::microseconds(1'000'000));
  }
  MonoTime MonoNow() const override {
    return MonoTime(std::chrono::microseconds(1'000'000));
  }
};

class MemoryRecorder final : public RecorderPort {
 public:
  bool TryPush(RecordEnvelope record) override {
    rows.push_back(std::move(record));
    return true;
  }
  std::vector<RecordEnvelope> rows;
};

TEST(PublicPaperTest, RestAndWebsocketDrivePaperThenDisconnectAndResync) {
  asio::io_context server_io;
  Tcp::acceptor http_acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  Tcp::acceptor ws_acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const auto http_port = std::to_string(http_acceptor.local_endpoint().port());
  const auto ws_port = std::to_string(ws_acceptor.local_endpoint().port());
  std::thread http_server([&] {
    for (int cycle = 0; cycle < 2; ++cycle) {
      Tcp::socket socket(server_io);
      http_acceptor.accept(socket);
      beast::flat_buffer buffer;
      http::request<http::string_body> request;
      http::read(socket, buffer, request);
      http::response<http::string_body> reply{http::status::ok, 11};
      reply.keep_alive(false);
      reply.body() =
          R"({"lastUpdateId":100,"bids":[["99.99","1.000"]],"asks":[["100.01","1.000"]]})";
      reply.prepare_payload();
      http::write(socket, reply);
    }
  });
  std::thread ws_server([&] {
    for (int cycle = 0; cycle < 2; ++cycle) {
      Tcp::socket socket(server_io);
      ws_acceptor.accept(socket);
      websocket::stream<Tcp::socket> ws(std::move(socket));
      ws.accept();
      ws.write(asio::buffer(std::string(
          R"({"e":"depthUpdate","s":"BTCUSDT","U":99,"u":101,"b":[],"a":[]})")));
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      ws.write(asio::buffer(std::string(
          R"({"e":"depthUpdate","s":"BTCUSDT","U":102,"u":103,"b":[["99.99","2.000"]],"a":[]})")));
    }
  });

  const FixedClock clock;
  const AccountId account("paper");
  const MarketId market{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
  const MarketSpec spec{market, AssetId("BTC"), AssetId("USDT")};
  const OwnerId owner{1, StrategyId("simple_pmm"), std::nullopt};
  const BookScale scale{D("0.01"), D("0.001"), 1};
  const TradingRule rule{market,    D("0.01"), D("0.001"), D("0.001"),
                         D("0.01"), {},        1,          clock.UtcNow()};
  SimplePmm strategy({owner, account, spec, D("0.01"), D("0.001"), D("0.001"),
                      std::chrono::seconds(15), PmmPriceType::Mid, D("0.001"),
                      true});
  PaperConnector paper({account,
                        spec,
                        rule,
                        {{"BTC", D("0.02")}, {"USDT", D("10")}},
                        D("0.001"),
                        true,
                        {}},
                       clock);
  RiskGate risk({ShardId{0}, D("0"), std::chrono::seconds(300)});
  ASSERT_TRUE(
      risk.SetInitialLease({account, AssetId("BTC"), ShardId{0}, 1, D("0.02"),
                            clock.UtcNow() + std::chrono::hours(1)})
          .ok());
  ASSERT_TRUE(
      risk.SetInitialLease({account, AssetId("USDT"), ShardId{0}, 1, D("10"),
                            clock.UtcNow() + std::chrono::hours(1)})
          .ok());
  MemoryRecorder recorder;
  ShardRuntime shard({RunId{1}, ShardId{0}, owner, account, spec, scale, rule},
                     clock, strategy, paper, risk, recorder);

  asio::io_context io;
  HttpClient http_client(io, "127.0.0.1", http_port);
  WebSocketClient websocket(io, "127.0.0.1", ws_port, "/ws/btcusdt@depth");
  binance_spot::StreamConfig stream_config;
  stream_config.symbol = "BTCUSDT";
  stream_config.connect_timeout = std::chrono::seconds(2);
  stream_config.snapshot_timeout = std::chrono::seconds(2);
  stream_config.read_timeout = std::chrono::seconds(2);
  absl::Status callback_status;
  binance_spot::MarketDataStream stream(
      stream_config, binance_spot::DepthParser(market, scale), http_client,
      websocket, shard.MutableBookSync(), clock,
      {[&](const BookApplyResult&) {
         auto status = shard.OnBookApplied();
         if (!status.ok()) callback_status = status;
       },
       [&](const PublicTrade& trade) {
         auto status = shard.OnPublicTrade(trade);
         if (!status.ok()) callback_status = status;
       },
       {}});

  auto cycle = [&]() {
    absl::Status outcome;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
          outcome = co_await stream.RunCycle(2);
        },
        asio::detached);
    io.run();
    io.restart();
    return outcome;
  };
  const auto first = cycle();
  ASSERT_TRUE(first.ok()) << first;
  ASSERT_TRUE(callback_status.ok()) << callback_status;
  ASSERT_EQ(shard.Book().State(), BookSyncState::Live);
  ASSERT_TRUE(shard.OnTimer({0, 1}).ok());
  auto opening = shard.OnTimer({1, 1});
  ASSERT_TRUE(opening.ok()) << opening.status();
  ASSERT_EQ(opening->size(), 2);
  EXPECT_TRUE((*opening)[0].accepted);
  EXPECT_TRUE((*opening)[1].accepted);
  ASSERT_EQ(paper.OpenOrders().size(), 2);

  shard.MutableBookSync().OnDisconnect();
  auto paused = shard.OnTimer({15'000'001, 1});
  ASSERT_TRUE(paused.ok()) << paused.status();
  EXPECT_EQ(shard.Book().State(), BookSyncState::Resyncing);
  EXPECT_EQ(paper.OpenOrders().size(), 0);
  EXPECT_EQ(paused->size(),
            2);  // cancel remains allowed while new orders pause

  const auto second = cycle();
  EXPECT_TRUE(second.ok()) << second;
  EXPECT_TRUE(callback_status.ok()) << callback_status;
  EXPECT_EQ(stream.StreamEpoch(), 2);
  EXPECT_EQ(shard.Book().State(), BookSyncState::Live);
  auto resumed = shard.OnTimer({30'000'001, 1});
  ASSERT_TRUE(resumed.ok()) << resumed.status();
  ASSERT_EQ(resumed->size(), 2);
  EXPECT_TRUE((*resumed)[0].accepted);
  EXPECT_TRUE((*resumed)[1].accepted);
  EXPECT_EQ(paper.OpenOrders().size(), 2);
  EXPECT_TRUE(shard.local_gaps().empty());
  http_server.join();
  ws_server.join();
}

}  // namespace
}  // namespace hquant
