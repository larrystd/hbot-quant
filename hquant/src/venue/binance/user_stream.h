#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "boost/asio/awaitable.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/steady_timer.hpp"
#include "hquant/base/net.h"
#include "hquant/config.h"
#include "hquant/trading/trading_event.h"

namespace hquant::v1 {

class TradePerf;

// Parses Binance Spot WS API user-data frames, including its event wrapper.
absl::StatusOr<std::vector<AccountEvent>> ParseBinanceUserFrame(
    std::string_view frame);

class BinanceUserStream {
 public:
  using EventSink = std::function<void(AccountEvent)>;
  using HealthSink = std::function<void(bool, std::string)>;

  BinanceUserStream(boost::asio::io_context& io, const AccountConfig& config,
                    std::string api_key, std::string api_secret,
                    EventSink on_event, HealthSink on_health,
                    TradePerf* perf = nullptr);
  void Start();
  void Stop();

 private:
  boost::asio::awaitable<void> Run();
  boost::asio::awaitable<absl::Status> RunOnce();

  boost::asio::io_context& io_;
  WebSocketClient websocket_;
  boost::asio::steady_timer reconnect_timer_;
  std::string api_key_;
  std::string api_secret_;
  EventSink on_event_;
  HealthSink on_health_;
  TradePerf* perf_ = nullptr;
  uint64_t next_frame_seq_ = 0;
  bool stopped_ = false;
};

}  // namespace hquant::v1
