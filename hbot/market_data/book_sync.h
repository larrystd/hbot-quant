#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>

#include "hbot/model/book_event.h"

namespace hbot {

enum class BookSyncState {
  Subscribing,
  Buffering,
  Replaying,
  Live,
  Stale,
  Resyncing
};
enum class BookApplyReason {
  None,
  OldDiff,
  Gap,
  CrossedBook,
  InvalidScale,
  InvalidMessage,
  BufferOverflow,
  Disconnected,
  Expired
};

struct BookApplyResult {
  BookSyncState state = BookSyncState::Subscribing;
  bool applied = false;
  std::optional<uint64_t> last_sequence;
  bool top_changed = false;
  BookApplyReason reason = BookApplyReason::None;
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
                         BookApplyReason reason = BookApplyReason::None,
                         bool top_changed = false);
  BookApplyResult Resync(BookApplyReason reason);
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

}  // namespace hbot
