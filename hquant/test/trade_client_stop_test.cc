#include <chrono>
#include <cstddef>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "boost/asio/io_context.hpp"
#include "boost/asio/steady_timer.hpp"
#include "hquant/venue/binance/trade_client.h"

int main() {
  using namespace hquant::v1;
  constexpr size_t kJobs = 64;
  for (int round = 0; round < 4; ++round) {
    boost::asio::io_context account_io;
    AccountConfig config;
    config.mode = TradingMode::PressureSim;
    config.trading_rest_host = "127.0.0.1";
    config.trading_rest_port = 1;
    config.private_ws_host = "127.0.0.1";
    config.private_ws_port = 1;
    config.rest_connections = 1;
    config.tls = false;
    BinanceTradeClient client(account_io, config, "key", "secret",
                              [](AccountEvent) {},
                              [](bool, std::string) {}, nullptr, 512);

    std::vector<unsigned> completions(kJobs);
    size_t total = 0;
    boost::asio::steady_timer deadline(account_io);
    deadline.expires_after(std::chrono::seconds(5));
    bool timed_out = false;
    deadline.async_wait([&](const boost::system::error_code& error) {
      if (!error) {
        timed_out = true;
        account_io.stop();
      }
    });
    for (size_t i = 0; i < kJobs; ++i) {
      SubmitCommand command{
          MarketId{"BTCUSDT"},
          ClientOrderId{"stop-test-" + std::to_string(round) + "-" +
                        std::to_string(i)},
          LimitOrderRequest{Side::Buy, 1'000'000'000, 1'000'000'000}};
      if (!client.Submit(std::move(command), [&, i](TradeResponse) {
            ++completions[i];
            if (++total == kJobs) deadline.cancel();
          })) {
        std::fprintf(stderr, "submit rejected in round %d\n", round);
        return 1;
      }
    }
    client.Stop();
    account_io.run();
    if (timed_out || total != kJobs || client.QueueHealth().queued != 0) {
      std::fprintf(stderr, "incomplete stop callbacks in round %d: %zu\n",
                   round, total);
      return 1;
    }
    for (unsigned count : completions) {
      if (count != 1) {
        std::fprintf(stderr, "duplicate or missing stop callback\n");
        return 1;
      }
    }
  }
  return 0;
}
