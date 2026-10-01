#include "hbot/market_data/book_sync.h"

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

#include "hbot/market_data/book_view.h"
#include "hbot/market_data/order_book.h"

namespace hbot {
namespace {

using Top = std::array<BookLevel, 2>;
struct VisibleTop {
  Top bids{};
  Top asks{};
  size_t bid_count = 0;
  size_t ask_count = 0;
};

VisibleTop CaptureTop(const BookView& view) {
  VisibleTop top;
  top.bid_count = view.CopyTopN(Side::Buy, top.bids);
  top.ask_count = view.CopyTopN(Side::Sell, top.asks);
  return top;
}

bool Different(const VisibleTop& left, const VisibleTop& right) {
  if (left.bid_count != right.bid_count || left.ask_count != right.ask_count)
    return true;
  for (size_t index = 0; index < left.bid_count; ++index) {
    if (left.bids[index].price_ticks != right.bids[index].price_ticks ||
        left.bids[index].quantity_lots != right.bids[index].quantity_lots)
      return true;
  }
  for (size_t index = 0; index < left.ask_count; ++index) {
    if (left.asks[index].price_ticks != right.asks[index].price_ticks ||
        left.asks[index].quantity_lots != right.asks[index].quantity_lots)
      return true;
  }
  return false;
}

uint64_t ReceiveUs(const EventTime& time) {
  const auto count = time.receive_mono.time_since_epoch().count();
  return count > 0 ? static_cast<uint64_t>(count) : 0;
}

}  // namespace

BookSync::BookSync(MarketId market, uint64_t scale_version, size_t dense_span,
                   size_t max_buffered_diffs, uint64_t stale_after_us)
    : market_(std::move(market)),
      scale_version_(scale_version),
      max_buffered_diffs_(std::max<size_t>(max_buffered_diffs, 1)),
      stale_after_us_(std::max<uint64_t>(stale_after_us, 1)),
      book_(std::make_unique<OrderBook>(dense_span)) {}

BookSync::~BookSync() = default;
BookSync::BookSync(BookSync&&) noexcept = default;
BookSync& BookSync::operator=(BookSync&&) noexcept = default;

BookApplyResult BookSync::Result(bool applied, BookApplyReason reason,
                                 bool top_changed) {
  book_->SetMetadata(state_, sequence_);
  return BookApplyResult{state_, applied, sequence_, top_changed, reason};
}

BookApplyResult BookSync::Resync(BookApplyReason reason) {
  state_ = BookSyncState::Resyncing;
  book_->Reset();
  buffered_diffs_.clear();
  return Result(false, reason);
}

BookApplyResult BookSync::Subscribe(uint64_t stream_epoch) {
  if (stream_epoch == 0) return Resync(BookApplyReason::InvalidMessage);
  stream_epoch_ = stream_epoch;
  sequence_.reset();
  last_update_us_ = 0;
  buffered_diffs_.clear();
  book_->Reset();
  state_ = BookSyncState::Buffering;
  return Result();
}

bool BookSync::ValidLevels(const std::vector<BookLevel>& levels,
                           bool allow_zero) const {
  for (const auto& level : levels) {
    if (level.price_ticks.value <= 0 ||
        (!allow_zero && level.quantity_lots.value == 0))
      return false;
  }
  return true;
}

BookApplyResult BookSync::OnSnapshot(const BookSnapshot& snapshot) {
  if (state_ != BookSyncState::Buffering &&
      state_ != BookSyncState::Replaying && state_ != BookSyncState::Live &&
      state_ != BookSyncState::Stale) {
    return Result(false, BookApplyReason::InvalidMessage);
  }
  if (snapshot.market != market_)
    return Result(false, BookApplyReason::InvalidMessage);
  if (snapshot.stream_epoch < stream_epoch_)
    return Result(false, BookApplyReason::OldDiff);
  if (snapshot.scale_version != scale_version_)
    return Resync(BookApplyReason::InvalidScale);
  if (snapshot.stream_epoch != stream_epoch_ ||
      snapshot.last_sequence == std::numeric_limits<uint64_t>::max() ||
      !ValidLevels(snapshot.bids, false) ||
      !ValidLevels(snapshot.asks, false)) {
    return Resync(BookApplyReason::InvalidMessage);
  }

  const VisibleTop previous = CaptureTop(*book_);
  book_->ApplySnapshot(snapshot.bids, snapshot.asks);
  if (book_->IsCrossed()) return Resync(BookApplyReason::CrossedBook);
  sequence_ = snapshot.last_sequence;
  last_update_us_ = ReceiveUs(snapshot.time);
  state_ = BookSyncState::Replaying;
  book_->SetMetadata(state_, sequence_);

  while (!buffered_diffs_.empty()) {
    const BookDiff diff = std::move(buffered_diffs_.front());
    buffered_diffs_.pop_front();
    if (diff.last_sequence <= *sequence_) continue;
    if (diff.first_sequence > *sequence_ + 1)
      return Resync(BookApplyReason::Gap);
    if (diff.scale_version != scale_version_ ||
        diff.stream_epoch != stream_epoch_) {
      return Resync(BookApplyReason::InvalidMessage);
    }
    book_->ApplyDiff(diff.bids, diff.asks);
    if (book_->IsCrossed()) return Resync(BookApplyReason::CrossedBook);
    sequence_ = diff.last_sequence;
    last_update_us_ = std::max(last_update_us_, ReceiveUs(diff.time));
    state_ = BookSyncState::Live;
  }
  book_->SetMetadata(state_, sequence_);
  return Result(true, BookApplyReason::None,
                Different(previous, CaptureTop(*book_)));
}

BookApplyResult BookSync::OnDiff(const BookDiff& diff) {
  if (diff.market != market_)
    return Result(false, BookApplyReason::InvalidMessage);
  if (diff.stream_epoch < stream_epoch_)
    return Result(false, BookApplyReason::OldDiff);
  if (diff.scale_version != scale_version_)
    return Resync(BookApplyReason::InvalidScale);
  if (diff.stream_epoch != stream_epoch_ || diff.first_sequence == 0 ||
      diff.first_sequence > diff.last_sequence ||
      diff.last_sequence == std::numeric_limits<uint64_t>::max() ||
      !ValidLevels(diff.bids, true) || !ValidLevels(diff.asks, true)) {
    return Resync(BookApplyReason::InvalidMessage);
  }
  if (state_ == BookSyncState::Buffering) {
    if (buffered_diffs_.size() >= max_buffered_diffs_) {
      return Resync(BookApplyReason::BufferOverflow);
    }
    buffered_diffs_.push_back(diff);
    return Result();
  }
  if (state_ != BookSyncState::Replaying && state_ != BookSyncState::Live &&
      state_ != BookSyncState::Stale) {
    return Result(false, BookApplyReason::InvalidMessage);
  }
  if (!sequence_ || *sequence_ == std::numeric_limits<uint64_t>::max()) {
    return Resync(BookApplyReason::InvalidMessage);
  }
  if (diff.last_sequence <= *sequence_)
    return Result(false, BookApplyReason::OldDiff);
  if (diff.first_sequence > *sequence_ + 1) return Resync(BookApplyReason::Gap);

  const VisibleTop previous = CaptureTop(*book_);
  book_->ApplyDiff(diff.bids, diff.asks);
  if (book_->IsCrossed()) return Resync(BookApplyReason::CrossedBook);
  sequence_ = diff.last_sequence;
  last_update_us_ = ReceiveUs(diff.time);
  state_ = BookSyncState::Live;
  book_->SetMetadata(state_, sequence_);
  return Result(true, BookApplyReason::None,
                Different(previous, CaptureTop(*book_)));
}

BookApplyResult BookSync::OnTimer(uint64_t now_us) {
  if (state_ != BookSyncState::Live && state_ != BookSyncState::Stale)
    return Result();
  if (now_us <= last_update_us_ ||
      now_us - last_update_us_ <= stale_after_us_) {
    return Result();
  }
  if (state_ == BookSyncState::Live) {
    state_ = BookSyncState::Stale;
    return Result(false, BookApplyReason::Expired);
  }
  if (now_us - last_update_us_ > stale_after_us_ &&
      now_us - last_update_us_ - stale_after_us_ > stale_after_us_) {
    return Resync(BookApplyReason::Expired);
  }
  return Result(false, BookApplyReason::Expired);
}

BookApplyResult BookSync::OnDisconnect() {
  return Resync(BookApplyReason::Disconnected);
}

BookApplyResult BookSync::OnInvalidScale() {
  return Resync(BookApplyReason::InvalidScale);
}

const BookView& BookSync::View() const { return *book_; }

}  // namespace hbot
