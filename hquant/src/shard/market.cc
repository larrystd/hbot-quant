#include "hquant/shard/market.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <type_traits>
#include <utility>
#include <variant>

#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/redirect_error.hpp"
#include "boost/asio/use_awaitable.hpp"

namespace hquant::v1 {

Market::Market(boost::asio::io_context& io, const MarketConfig& config,
               const InputConfig& input,
               std::function<void(const MarketNotice&)> notify)
    : io_(io),
      config_(config),
      input_(input),
      notify_(std::move(notify)),
      stale_timer_(io),
      feed_(io, config, input,
            [this](const MarketInput& value, MonoTime now) {
              HandleInput(value, now);
              return needs_resync_;
            },
            [this](std::string error) { last_error_ = std::move(error); }) {}

void Market::RefreshView() {
  view_.last_sequence = sequence_;
  if (view_.state == BookState::Live) {
    view_.best_bid = book_.BestBid();
    view_.best_ask = book_.BestAsk();
  } else {
    view_.best_bid.reset();
    view_.best_ask.reset();
  }
}

// 清空盘口、缓冲和序号，回到 Syncing。reconnect=true 表示当前连接已不可信：
// 实时模式下 BinanceFeed 从回调返回值看到它会断线重连；在下一次 OnConnected
// 之前，本连接后续的快照和增量都会被忽略。
void Market::EnterSyncing(std::string reason, bool reconnect) {
  const bool was_live = view_.state == BookState::Live;
  view_.state = BookState::Syncing;
  book_.Reset();
  buffered_diffs_.clear();
  sequence_.reset();
  snapshot_ready_ = false;
  view_.last_book_at.reset();
  needs_resync_ = reconnect;
  last_error_ = std::move(reason);
  RefreshView();
  if (was_live) notify_(MarketNotice{MarketNotice::Kind::BookUnavailable, {}});
}

void Market::OnConnected(const FeedConnected& input) {
  if (input.connection_epoch == 0 || input.connection_epoch <= connection_epoch_) return;
  EnterSyncing("new connection", false);
  connection_epoch_ = input.connection_epoch;
  view_.last_trade.reset();
}

// 用快照序号对齐缓冲的增量。没有快照时只缓存不处理；至少接上一条增量后
// 才切换到 Live，单有快照不算 Live。
void Market::ApplyBuffered(MonoTime now) {
  if (!snapshot_ready_ || !sequence_) return;
  bool applied = false;
  while (!buffered_diffs_.empty()) {
    BookDiff diff = std::move(buffered_diffs_.front());
    buffered_diffs_.pop_front();
    if (diff.last_sequence <= *sequence_) continue;  // already covered
    if (diff.first_sequence > *sequence_ + 1) {
      EnterSyncing("depth sequence gap", true);
      return;
    }
    if (!book_.ApplyDiff(diff.bids, diff.asks)) {
      EnterSyncing("invalid depth book", true);
      return;
    }
    sequence_ = diff.last_sequence;
    view_.last_book_at = now;
    applied = true;
  }
  if (applied) {
    view_.state = BookState::Live;
    last_error_.clear();
    RefreshView();
    notify_(MarketNotice{MarketNotice::Kind::BookChanged, {}});
  } else {
    RefreshView();
  }
}

void Market::OnSnapshot(const BookSnapshot& input, MonoTime now) {
  if (connection_epoch_ == 0 || input.connection_epoch != connection_epoch_ ||
      view_.state == BookState::Live || needs_resync_) return;
  if (input.market != config_.id) {
    EnterSyncing("snapshot for wrong market", true);
    return;
  }
  if (input.last_sequence == 0 ||
      input.last_sequence == std::numeric_limits<uint64_t>::max() ||
      !book_.ApplySnapshot(input.bids, input.asks)) {
    EnterSyncing("invalid snapshot", true);
    return;
  }
  sequence_ = input.last_sequence;
  snapshot_ready_ = true;
  view_.last_book_at = now;
  ApplyBuffered(now);
}

// 增量带区间 [first_sequence, last_sequence]，sequence_ 是本地已应用到的序号。
// 能衔接的条件是 first <= sequence_ + 1 <= last。区间与已应用部分重叠是
// 安全的，因为每个价位给的是绝对数量，重复应用结果不变。
// Live 时直接应用；Syncing 时先缓冲，交给 ApplyBuffered 等快照对齐。
void Market::OnDiff(const BookDiff& input, MonoTime now) {
  // 未连接、旧连接残留、或已在等待重连：静默丢弃。
  if (connection_epoch_ == 0 || input.connection_epoch != connection_epoch_ ||
      needs_resync_) return;
  if (input.market != config_.id) {
    EnterSyncing("depth for wrong market", true);
    return;
  }
  if (input.first_sequence == 0 ||
      input.first_sequence > input.last_sequence ||
      input.last_sequence == std::numeric_limits<uint64_t>::max()) {
    EnterSyncing("invalid depth interval", true);
    return;
  }
  if (view_.state == BookState::Live) {
    if (input.last_sequence <= *sequence_) return;  // already covered
    // 序号缺口，或应用后盘口交叉/单边为空，说明本地盘口已与交易所不一致。
    if (input.first_sequence > *sequence_ + 1 ||
        !book_.ApplyDiff(input.bids, input.asks)) {
      EnterSyncing("depth gap or invalid book", true);
      return;
    }
    sequence_ = input.last_sequence;
    view_.last_book_at = now;
    RefreshView();
    notify_(MarketNotice{MarketNotice::Kind::BookChanged, {}});
    return;
  }
  // 缓冲满说明快照迟迟未到，放弃本轮同步。
  if (buffered_diffs_.size() >= config_.max_buffered_diffs) {
    EnterSyncing("depth buffer full", true);
    return;
  }
  buffered_diffs_.push_back(input);
  ApplyBuffered(now);
}

void Market::OnTrade(const PublicTrade& input) {
  if (connection_epoch_ == 0 || input.connection_epoch != connection_epoch_ ||
      input.market != config_.id) return;
  if (input.price_ticks == 0 || input.quantity_lots == 0) {
    last_error_ = "invalid public trade";
    return;
  }
  view_.last_trade = BookLevel{input.price_ticks, input.quantity_lots};
  notify_(MarketNotice{MarketNotice::Kind::PublicTrade, input});
}

void Market::OnDisconnected(const FeedDisconnected& input) {
  if (input.connection_epoch != connection_epoch_) return;
  EnterSyncing(input.reason, false);
}

void Market::HandleInput(const MarketInput& input, MonoTime now) {
  if (stopped_) return;
  std::visit([this, now](const auto& value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, FeedConnected>) OnConnected(value);
    if constexpr (std::is_same_v<T, BookSnapshot>) OnSnapshot(value, now);
    if constexpr (std::is_same_v<T, BookDiff>) OnDiff(value, now);
    if constexpr (std::is_same_v<T, PublicTrade>) OnTrade(value);
    if constexpr (std::is_same_v<T, FeedDisconnected>) OnDisconnected(value);
  }, input);
}

void Market::OnReplayInput(const MarketInput& input, MonoTime now) {
  HandleInput(input, now);
}

bool Market::CheckStale(MonoTime now) {
  if (stopped_ || view_.state != BookState::Live || !view_.last_book_at ||
      now < *view_.last_book_at ||
      now - *view_.last_book_at <= std::chrono::milliseconds(config_.stale_after_ms)) {
    return false;
  }
  EnterSyncing("book stale", true);
  if (input_.source == InputSource::BinancePublic) feed_.RequestResync();
  return true;
}

boost::asio::awaitable<void> Market::StaleLoop() {
  const auto interval = std::chrono::milliseconds(
      std::max<int64_t>(1, std::min<int64_t>(config_.stale_after_ms / 2, 1000)));
  while (!stopped_) {
    stale_timer_.expires_after(interval);
    boost::system::error_code error;
    co_await stale_timer_.async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, error));
    if (error || stopped_) break;
    CheckStale(std::chrono::time_point_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now()));
  }
}

boost::asio::awaitable<void> Market::RunBinance() {
  boost::asio::co_spawn(io_, StaleLoop(), boost::asio::detached);
  co_await feed_.Run();
  stale_timer_.cancel();
}

void Market::Stop() {
  if (stopped_) return;
  stopped_ = true;
  feed_.Stop();
  stale_timer_.cancel();
  view_.state = BookState::Syncing;
  book_.Reset();
  sequence_.reset();
  RefreshView();
}

}  // namespace hquant::v1
