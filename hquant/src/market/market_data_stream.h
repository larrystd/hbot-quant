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

namespace hquant {

// 行情的接收方。行情模块只负责收消息、解析、管理连接；解析好的数据交给它。
// 它负责改订单簿，并返回结果：行情模块据此决定继续收消息，还是断开重新同步。
// 所有方法都在行情模块所在的线程（分片线程）上调用。
class MarketDataReceiver {
 public:
  virtual ~MarketDataReceiver() = default;
  // 新连接开始：之前连接的数据作废，订单簿等待快照。
  virtual BookApplyResult OnConnect(uint64_t connection_id) = 0;
  virtual BookApplyResult OnSnapshot(const BookSnapshot& snapshot) = 0;
  virtual BookApplyResult OnDiff(const BookDiff& diff) = 0;
  // 连接出错断开：订单簿不再可信。
  virtual BookApplyResult OnDisconnect() = 0;
  virtual void OnPublicTrade(const PublicTrade& trade) = 0;
  // 连接、解析或同步出错。行情模块随后会自己重连。
  virtual void OnStreamError(const absl::Status& status) = 0;
};

}  // namespace hquant

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

// One cycle opens a fresh WS epoch, buffers the first depth interval, obtains
// an async REST snapshot, and then applies the buffered and live intervals.
// A gap/fault returns a status so Run can reconnect and resynchronize.
class MarketDataStream {
 public:
  MarketDataStream(StreamConfig config, DepthParser parser,
                   HttpTransport& snapshot_http, WebSocketClient& websocket,
                   MarketDataReceiver& receiver, const Clock& clock);

  boost::asio::awaitable<absl::Status> RunCycle(size_t max_depth_messages = 0);
  boost::asio::awaitable<void> Run();
  void Stop();
  uint64_t StreamEpoch() const { return epoch_; }
  uint64_t AppliedDiffs() const { return applied_diffs_; }
  uint64_t Resyncs() const { return resyncs_; }

 private:
  EventTime Now() const;
  // 记下接收方返回的同步状态，出错时据此判断是否还需要通知断开。
  void Track(const BookApplyResult& result);
  absl::Status Fault(absl::Status status);

  StreamConfig config_;
  DepthParser parser_;
  HttpTransport& snapshot_http_;
  WebSocketClient& websocket_;
  MarketDataReceiver& receiver_;
  const Clock& clock_;
  BookSyncState book_state_ = BookSyncState::Subscribing;
  uint64_t epoch_ = 0;
  uint64_t applied_diffs_ = 0;
  uint64_t resyncs_ = 0;
  bool stopped_ = false;
  bool cycle_became_live_ = false;
  std::chrono::steady_clock::duration retry_after_{};
};

}  // namespace hquant::binance_spot
