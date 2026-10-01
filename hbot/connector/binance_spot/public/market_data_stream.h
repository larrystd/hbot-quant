#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <string>

#include "absl/status/status.h"
#include "boost/asio/awaitable.hpp"
#include "hbot/base/clock.h"
#include "hbot/connector/binance_spot/public/depth_parser.h"
#include "hbot/market_data/book_sync.h"
#include "hbot/net/api.h"
#include "hbot/net/websocket_client.h"

namespace hbot::binance_spot {

struct StreamConfig {
  std::string symbol;
  unsigned snapshot_limit = 1000;
  unsigned max_snapshot_attempts = 3;
  std::chrono::steady_clock::duration connect_timeout = std::chrono::seconds(5);
  std::chrono::steady_clock::duration snapshot_timeout =
      std::chrono::seconds(5);
  std::chrono::steady_clock::duration read_timeout = std::chrono::seconds(30);
  std::chrono::steady_clock::duration reconnect_delay =
      std::chrono::milliseconds(250);
  std::chrono::steady_clock::duration max_reconnect_delay =
      std::chrono::seconds(30);
};

struct StreamCallbacks {
  std::function<void(const BookApplyResult&)> on_book;
  std::function<void(const PublicTrade&)> on_trade;
  std::function<void(const absl::Status&)> on_error;
};

// One cycle opens a fresh WS epoch, buffers the first depth interval, obtains
// an async REST snapshot, and then applies the buffered and live intervals.
// A gap/fault returns a status so Run can reconnect and resynchronize.
class MarketDataStream {
 public:
  MarketDataStream(StreamConfig config, DepthParser parser,
                   HttpTransport& snapshot_http, WebSocketClient& websocket,
                   BookSync& book, const Clock& clock,
                   StreamCallbacks callbacks = {});

  boost::asio::awaitable<absl::Status> RunCycle(size_t max_depth_messages = 0);
  boost::asio::awaitable<void> Run();
  void Stop();
  uint64_t StreamEpoch() const { return epoch_; }

 private:
  EventTime Now() const;
  void Report(const BookApplyResult& result) const;
  absl::Status Fault(absl::Status status);

  StreamConfig config_;
  DepthParser parser_;
  HttpTransport& snapshot_http_;
  WebSocketClient& websocket_;
  BookSync& book_;
  const Clock& clock_;
  StreamCallbacks callbacks_;
  uint64_t epoch_ = 0;
  bool stopped_ = false;
  bool cycle_became_live_ = false;
};

}  // namespace hbot::binance_spot
