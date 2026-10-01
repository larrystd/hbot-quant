#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/net.h"
#include "base/types.h"
#include "boost/asio/awaitable.hpp"
#include "market/order_book.h"

namespace hquant::binance_spot {

// Parses Binance Spot JSON only. Fixed TickLotSize is checked before any
// integer conversion; no floating-point price or quantity enters the book.
class DepthParser {
 public:
  DepthParser(MarketId market, TickLotSize scale)
      : market_(std::move(market)), scale_(std::move(scale)) {}

  absl::StatusOr<BookSnapshot> ParseSnapshot(std::string_view json,
                                             uint64_t connection_id,
                                             EventTime received) const;
  absl::StatusOr<BookDiff> ParseDiff(std::string_view json,
                                     uint64_t connection_id,
                                     EventTime received) const;
  absl::StatusOr<PublicTrade> ParseTrade(std::string_view json,
                                         EventTime received) const;

  const MarketId& Market() const { return market_; }
  const TickLotSize& Scale() const { return scale_; }

 private:
  MarketId market_;
  TickLotSize scale_;
};

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
                   OrderBookSync& book, const Clock& clock,
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
  OrderBookSync& book_;
  const Clock& clock_;
  StreamCallbacks callbacks_;
  uint64_t epoch_ = 0;
  bool stopped_ = false;
  bool cycle_became_live_ = false;
  std::chrono::steady_clock::duration retry_after_{};
};

}  // namespace hquant::binance_spot
