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
  Buffering,
  Replaying,
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

class BookView;
class OrderBook;

// Owns synchronization and one mutable book on its shard thread. The adapter
// supplies normalized, ordered sequence intervals and integer levels.
class BookSync {
 public:
  BookSync(MarketId market, uint64_t scale_version, size_t dense_span = 8192,
           size_t max_buffered_diffs = 1024,
           uint64_t stale_after_us = 5'000'000);
  ~BookSync();
  BookSync(BookSync&&) noexcept;
  BookSync& operator=(BookSync&&) noexcept;
  BookSync(const BookSync&) = delete;
  BookSync& operator=(const BookSync&) = delete;

  BookApplyResult Subscribe(uint64_t stream_epoch);
  BookApplyResult OnSnapshot(const BookSnapshot& snapshot);
  BookApplyResult OnDiff(const BookDiff& diff);
  BookApplyResult OnTimer(uint64_t now_us);
  BookApplyResult OnDisconnect();
  BookApplyResult OnInvalidScale();
  const BookView& View() const;

 private:
  BookApplyResult Result(bool applied = false,
                         ErrorCode reason = ErrorCode::kOk,
                         bool top_changed = false,
                         BookApplyOutcome outcome = BookApplyOutcome::Normal);
  BookApplyResult Resync(ErrorCode reason);
  bool ValidLevels(const std::vector<BookLevel>& levels, bool allow_zero) const;

  MarketId market_;
  uint64_t scale_version_;
  uint64_t stream_epoch_ = 0;
  size_t max_buffered_diffs_;
  uint64_t stale_after_us_;
  uint64_t last_update_us_ = 0;
  BookSyncState state_ = BookSyncState::Subscribing;
  std::optional<uint64_t> sequence_;
  std::deque<BookDiff> buffered_diffs_;
  std::unique_ptr<OrderBook> book_;
};

// Borrowed view: valid only in the owning shard's synchronous callback.
class BookView {
 public:
  virtual ~BookView() = default;
  virtual BookSyncState State() const = 0;
  virtual std::optional<uint64_t> LastSequence() const = 0;
  virtual std::optional<BookLevel> BestBid() const = 0;
  virtual std::optional<BookLevel> BestAsk() const = 0;
  virtual size_t CopyTopN(Side side, std::span<BookLevel> output) const = 0;
};

// One shard owns this mutable L2 book. The dense ladder covers nearby ticks;
// distant prices live in the ordered overflow maps. Quantities are absolute.
class OrderBook final : public BookView {
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
