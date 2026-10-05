#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <optional>
#include <string>

#include "boost/asio/awaitable.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/steady_timer.hpp"
#include "hquant/config.h"
#include "hquant/shard/binance_feed.h"
#include "hquant/shard/market_type.h"
#include "hquant/shard/order_book.h"

namespace hquant::v1 {

class Market {
 public:
  Market(boost::asio::io_context& io, const MarketConfig& config,
         const InputConfig& input, std::function<void(const MarketNotice&)> notify);
  Market(const Market&) = delete;
  Market& operator=(const Market&) = delete;

  boost::asio::awaitable<void> RunBinance();
  void OnReplayInput(const MarketInput& input, MonoTime now);
  bool CheckStale(MonoTime now);
  const BookView& View() const { return view_; }
  bool NeedsResync() const { return needs_resync_; }
  const std::string& LastError() const { return last_error_; }
  void Stop();

 private:
  boost::asio::awaitable<void> StaleLoop();
  void HandleInput(const MarketInput& input, MonoTime now);
  void EnterSyncing(std::string reason, bool reconnect);
  void RefreshView();
  void ApplyBuffered(MonoTime now);
  void OnConnected(const FeedConnected& input);
  // 用完整快照重建盘口，作为增量的起点。只在 Syncing 时生效，Live 时忽略。
  // 本身不切换 Live，需经 ApplyBuffered 接上至少一条增量。
  void OnSnapshot(const BookSnapshot& input, MonoTime now);
  // 用增量更新盘口。Live 时直接应用并通知 BookChanged；Syncing 时缓冲，
  // 等快照对齐。序号缺口、盘口非法或缓冲满时清空盘口并要求重连。
  void OnDiff(const BookDiff& input, MonoTime now);
  // 转发公开成交给 Shard，供模拟交易所撮合。不改盘口，不受盘口状态影响；
  // 非法成交只记录错误，不触发重连。
  void OnTrade(const PublicTrade& input);
  void OnDisconnected(const FeedDisconnected& input);

  boost::asio::io_context& io_;
  MarketConfig config_;
  InputConfig input_;
  std::function<void(const MarketNotice&)> notify_;
  OrderBook book_;
  BookView view_;
  std::deque<BookDiff> buffered_diffs_;
  std::optional<uint64_t> sequence_;
  uint64_t connection_epoch_ = 0;
  bool snapshot_ready_ = false;
  bool needs_resync_ = false;
  bool stopped_ = false;
  std::string last_error_;
  boost::asio::steady_timer stale_timer_;
  BinanceFeed feed_;
};

}  // namespace hquant::v1
