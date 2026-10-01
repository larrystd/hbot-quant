#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "absl/container/btree_map.h"
#include "hbot/market_data/book_view.h"

namespace hbot {

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

}  // namespace hbot
