#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "absl/container/btree_map.h"
#include "base/error.h"
#include "base/market.h"

namespace hquant {

enum class BookSyncState {
  Subscribing,
  WaitingSnapshot,
  CatchingUp,
  Live,
  Stale,
  Resyncing
};
enum class BookApplyOutcome { Normal, OldDiff };

struct BookApplyResult {
  BookSyncState state = BookSyncState::Subscribing;
  bool applied = false;
  std::optional<uint64_t> last_sequence;
  bool top_changed = false;
  ErrorCode reason = ErrorCode::kOk;
  BookApplyOutcome outcome = BookApplyOutcome::Normal;
};

class OrderBookView;
class OrderBook;

// Owns synchronization and one mutable book on its shard thread. The adapter
// supplies normalized, ordered sequence intervals and integer levels.
class OrderBookSync {
 public:
  OrderBookSync(MarketId market, uint64_t tick_lot_version,
                size_t dense_span = 8192, size_t max_buffered_diffs = 1024,
                uint64_t stale_after_us = 5'000'000);
  ~OrderBookSync();
  OrderBookSync(OrderBookSync&&) noexcept;
  OrderBookSync& operator=(OrderBookSync&&) noexcept;
  OrderBookSync(const OrderBookSync&) = delete;
  OrderBookSync& operator=(const OrderBookSync&) = delete;

  BookApplyResult Subscribe(uint64_t connection_id);  // 初始化OrderBookSync
  BookApplyResult OnSnapshot(const BookSnapshot& snapshot);
  BookApplyResult OnDiff(const BookDiff& diff);
  BookApplyResult OnTimer(uint64_t now_us);
  BookApplyResult OnDisconnect();
  BookApplyResult OnInvalidScale();
  const OrderBookView& View() const;

 private:
  BookApplyResult Result(bool applied = false,
                         ErrorCode reason = ErrorCode::kOk,
                         bool top_changed = false,
                         BookApplyOutcome outcome = BookApplyOutcome::Normal);
  BookApplyResult Resync(ErrorCode reason);
  bool ValidLevels(const std::vector<BookLevel>& levels, bool allow_zero) const;

  MarketId market_;
  uint64_t tick_lot_version_;
  uint64_t connection_id_ = 0;  // 只接受这条connection的更新
  size_t max_buffered_diffs_;
  uint64_t stale_after_us_;  // 订单薄过期时间
  uint64_t last_update_us_ = 0;
  BookSyncState state_ = BookSyncState::Subscribing;
  std::optional<uint64_t> sequence_;  // 订单序号，递增
  std::deque<BookDiff>
      buffered_diffs_;  // 缓存增量数据，只在快照/订单薄尚未准备好时应用
  std::unique_ptr<OrderBook> book_;
};

// Borrowed view: valid only in the owning shard's synchronous callback.
class OrderBookView {
 public:
  virtual ~OrderBookView() = default;
  virtual BookSyncState State() const = 0;
  virtual std::optional<uint64_t> LastSequence() const = 0;
  virtual std::optional<BookLevel> BestBid() const = 0;
  virtual std::optional<BookLevel> BestAsk() const = 0;
  virtual size_t CopyTopN(Side side, std::span<BookLevel> output) const = 0;
};

// One shard owns this mutable L2 book. The dense ladder covers nearby ticks;
// distant prices live in the ordered overflow maps. Quantities are absolute.
class OrderBook final : public OrderBookView {
 public:
  explicit OrderBook(size_t dense_span = 8192);

  void Reset();
  void ApplySnapshot(std::span<const BookLevel> bids,
                     std::span<const BookLevel> asks);
  void ApplyDiff(std::span<const BookLevel> bids,
                 std::span<const BookLevel> asks);
  bool IsCrossed() const;
  void SetMetadata(BookSyncState state, std::optional<uint64_t> sequence);

  BookSyncState State() const override { return state_; }
  std::optional<uint64_t> LastSequence() const override { return sequence_; }
  std::optional<BookLevel> BestBid() const override;
  std::optional<BookLevel> BestAsk() const override;
  size_t CopyTopN(Side side, std::span<BookLevel> output) const override;

 private:
  void InitWindow(std::span<const BookLevel> bids,
                  std::span<const BookLevel> asks);
  void ApplyLevels(Side side, std::span<const BookLevel> levels);
  void RecenterIfNeeded();
  size_t CopyTopNRaw(Side side, std::span<BookLevel> output) const;
  std::optional<BookLevel> BestRaw(Side side) const;

  size_t dense_span_;
  int64_t dense_base_ = 1;
  std::vector<uint64_t> dense_bids_;
  std::vector<uint64_t> dense_asks_;
  absl::btree_map<int64_t, uint64_t> overflow_bids_;
  absl::btree_map<int64_t, uint64_t> overflow_asks_;
  BookSyncState state_ = BookSyncState::Subscribing;
  std::optional<uint64_t> sequence_;
};

}  // namespace hquant
