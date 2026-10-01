#include "hbot/connector/binance_spot/public/market_data_stream.h"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/beast/http.hpp"
#include "boost/beast/websocket.hpp"
#include "gtest/gtest.h"
#include "hbot/market_data/book_view.h"
#include "hbot/net/http_client.h"

namespace hbot::binance_spot {
namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using Tcp = asio::ip::tcp;

Decimal D(const char* text) { return *Decimal::Parse(text); }
MarketId Market() {
  return MarketId{VenueId("binance"), InstrumentKind::Spot, "BTCUSDT"};
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

void RunLocalCycle(uint64_t second_first, bool retry_snapshot,
                   absl::StatusCode expected_status, unsigned http_status = 200,
                   bool delay_http = false) {
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
  BookScale scale{D("0.01"), D("0.001"), 1};
  BookSync book(Market(), 1);
  TestClock clock;
  StreamConfig config;
  config.symbol = "BTCUSDT";
  config.snapshot_timeout =
      delay_http ? std::chrono::milliseconds(30) : std::chrono::seconds(2);
  config.read_timeout = std::chrono::seconds(2);
  std::vector<BookApplyResult> results;
  MarketDataStream stream(
      config, DepthParser(Market(), scale), http_client, websocket, book, clock,
      StreamCallbacks{
          [&](const BookApplyResult& result) { results.push_back(result); },
          {},
          {}});
  absl::Status outcome;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> { outcome = co_await stream.RunCycle(2); },
      asio::detached);
  io.run();
  http_server.join();
  ws_server.join();

  EXPECT_EQ(outcome.code(), expected_status) << outcome;
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
      if (result.reason == BookApplyReason::Gap) saw_gap = true;
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
  RunLocalCycle(102, false, absl::StatusCode::kResourceExhausted, 429);
}

TEST(MarketDataStreamTest, SnapshotDeadlineStopsCycle) {
  RunLocalCycle(102, false, absl::StatusCode::kDeadlineExceeded, 200, true);
}

}  // namespace
}  // namespace hbot::binance_spot
