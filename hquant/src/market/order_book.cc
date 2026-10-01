#include "market/order_book.h"

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

namespace hquant {
namespace {

using Top = std::array<BookLevel, 2>;
struct VisibleTop {
  Top bids{};
  Top asks{};
  size_t bid_count = 0;
  size_t ask_count = 0;
};

VisibleTop CaptureTop(const OrderBookView& view) {
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

OrderBookSync::OrderBookSync(MarketId market, uint64_t tick_lot_version,
                             size_t dense_span, size_t max_buffered_diffs,
                             uint64_t stale_after_us)
    : market_(std::move(market)),
      tick_lot_version_(tick_lot_version),
      max_buffered_diffs_(std::max<size_t>(max_buffered_diffs, 1)),
      stale_after_us_(std::max<uint64_t>(stale_after_us, 1)),
      book_(std::make_unique<OrderBook>(dense_span)) {}

OrderBookSync::~OrderBookSync() = default;
OrderBookSync::OrderBookSync(OrderBookSync&&) noexcept = default;
OrderBookSync& OrderBookSync::operator=(OrderBookSync&&) noexcept = default;

BookApplyResult OrderBookSync::Result(bool applied, ErrorCode reason,
                                      bool top_changed,
                                      BookApplyOutcome outcome) {
  book_->SetMetadata(state_, sequence_);
  return BookApplyResult{state_,      applied, sequence_,
                         top_changed, reason,  outcome};
}

BookApplyResult OrderBookSync::Resync(ErrorCode reason) {
  state_ = BookSyncState::Resyncing;
  book_->Reset();
  buffered_diffs_.clear();
  return Result(false, reason);
}

BookApplyResult OrderBookSync::Subscribe(uint64_t connection_id) {
  if (connection_id == 0) return Resync(ErrorCode::kFeedConfigInvalid);
  connection_id_ = connection_id;
  sequence_.reset();
  last_update_us_ = 0;
  buffered_diffs_.clear();
  book_->Reset();
  state_ = BookSyncState::WaitingSnapshot;
  return Result();
}

bool OrderBookSync::ValidLevels(const std::vector<BookLevel>& levels,
                                bool allow_zero) const {
  for (const auto& level : levels) {
    if (level.price_ticks.value <= 0 ||
        (!allow_zero && level.quantity_lots.value == 0))
      return false;
  }
  return true;
}

BookApplyResult OrderBookSync::OnSnapshot(const BookSnapshot& snapshot) {
  if (state_ != BookSyncState::WaitingSnapshot &&
      state_ != BookSyncState::CatchingUp && state_ != BookSyncState::Live &&
      state_ != BookSyncState::Stale) {
    return Result(false, ErrorCode::kFeedMessageInvalid);
  }
  if (snapshot.market != market_) return Resync(ErrorCode::kFeedWrongMarket);
  if (snapshot.connection_id < connection_id_)
    return Result(false, ErrorCode::kOk, false, BookApplyOutcome::OldDiff);
  if (snapshot.tick_lot_version != tick_lot_version_)
    return Resync(ErrorCode::kFeedTickSizeMismatch);
  if (snapshot.connection_id != connection_id_ ||
      snapshot.last_sequence == std::numeric_limits<uint64_t>::max() ||
      !ValidLevels(snapshot.bids, false) ||
      !ValidLevels(snapshot.asks, false)) {
    return Resync(ErrorCode::kFeedMessageInvalid);
  }

  const VisibleTop previous = CaptureTop(*book_);
  book_->ApplySnapshot(snapshot.bids, snapshot.asks);
  if (book_->IsCrossed()) return Resync(ErrorCode::kBookCrossed);
  sequence_ = snapshot.last_sequence;
  last_update_us_ = ReceiveUs(snapshot.time);
  state_ = BookSyncState::CatchingUp;
  book_->SetMetadata(state_, sequence_);

  while (!buffered_diffs_.empty()) {
    const BookDiff diff = std::move(buffered_diffs_.front());
    buffered_diffs_.pop_front();
    if (diff.last_sequence <= *sequence_) continue;
    if (diff.first_sequence > *sequence_ + 1)
      return Resync(ErrorCode::kBookSequenceGap);
    if (diff.tick_lot_version != tick_lot_version_ ||
        diff.connection_id != connection_id_) {
      return Resync(ErrorCode::kFeedMessageInvalid);
    }
    book_->ApplyDiff(diff.bids, diff.asks);
    if (book_->IsCrossed()) return Resync(ErrorCode::kBookCrossed);
    sequence_ = diff.last_sequence;
    last_update_us_ = std::max(last_update_us_, ReceiveUs(diff.time));
    state_ = BookSyncState::Live;
  }
  book_->SetMetadata(state_, sequence_);
  return Result(true, ErrorCode::kOk, Different(previous, CaptureTop(*book_)));
}

BookApplyResult OrderBookSync::OnDiff(const BookDiff& diff) {
  if (diff.market != market_) return Resync(ErrorCode::kFeedWrongMarket);
  if (diff.connection_id < connection_id_)
    return Result(false, ErrorCode::kOk, false, BookApplyOutcome::OldDiff);
  if (diff.tick_lot_version != tick_lot_version_)
    return Resync(ErrorCode::kFeedTickSizeMismatch);
  if (diff.connection_id != connection_id_ || diff.first_sequence == 0 ||
      diff.first_sequence > diff.last_sequence ||
      diff.last_sequence == std::numeric_limits<uint64_t>::max() ||
      !ValidLevels(diff.bids, true) || !ValidLevels(diff.asks, true)) {
    return Resync(ErrorCode::kFeedMessageInvalid);
  }
  if (state_ == BookSyncState::WaitingSnapshot) {
    if (buffered_diffs_.size() >= max_buffered_diffs_) {
      return Resync(ErrorCode::kBookBufferOverflow);
    }
    buffered_diffs_.push_back(diff);
    return Result();
  }
  if (state_ != BookSyncState::CatchingUp && state_ != BookSyncState::Live &&
      state_ != BookSyncState::Stale) {
    return Result(false, ErrorCode::kFeedMessageInvalid);
  }
  if (!sequence_ || *sequence_ == std::numeric_limits<uint64_t>::max()) {
    return Resync(ErrorCode::kFeedMessageInvalid);
  }
  if (diff.last_sequence <= *sequence_)
    return Result(false, ErrorCode::kOk, false, BookApplyOutcome::OldDiff);
  if (diff.first_sequence > *sequence_ + 1)
    return Resync(ErrorCode::kBookSequenceGap);

  const VisibleTop previous = CaptureTop(*book_);
  book_->ApplyDiff(diff.bids, diff.asks);
  if (book_->IsCrossed()) return Resync(ErrorCode::kBookCrossed);
  sequence_ = diff.last_sequence;
  last_update_us_ = ReceiveUs(diff.time);
  state_ = BookSyncState::Live;
  book_->SetMetadata(state_, sequence_);
  return Result(true, ErrorCode::kOk, Different(previous, CaptureTop(*book_)));
}

BookApplyResult OrderBookSync::OnTimer(uint64_t now_us) {
  if (state_ != BookSyncState::Live && state_ != BookSyncState::Stale)
    return Result();
  if (now_us <= last_update_us_ ||
      now_us - last_update_us_ <= stale_after_us_) {
    return Result();
  }
  if (state_ == BookSyncState::Live) {
    state_ = BookSyncState::Stale;
    return Result(false, ErrorCode::kBookStale);
  }
  if (now_us - last_update_us_ > stale_after_us_ &&
      now_us - last_update_us_ - stale_after_us_ > stale_after_us_) {
    return Resync(ErrorCode::kBookStale);
  }
  return Result(false, ErrorCode::kBookStale);
}

BookApplyResult OrderBookSync::OnDisconnect() {
  return Resync(ErrorCode::kFeedDisconnected);
}

BookApplyResult OrderBookSync::OnInvalidScale() {
  return Resync(ErrorCode::kFeedTickSizeMismatch);
}

const OrderBookView& OrderBookSync::View() const { return *book_; }

}  // namespace hquant

namespace hquant {

OrderBook::OrderBook(size_t dense_span)
    : dense_span_(std::clamp<size_t>(dense_span, 1, 1 << 20)),
      dense_bids_(dense_span_, 0),
      dense_asks_(dense_span_, 0) {}

void OrderBook::Reset() {
  std::fill(dense_bids_.begin(), dense_bids_.end(), 0);
  std::fill(dense_asks_.begin(), dense_asks_.end(), 0);
  overflow_bids_.clear();
  overflow_asks_.clear();
  dense_base_ = 1;
  sequence_.reset();
}

void OrderBook::InitWindow(std::span<const BookLevel> bids,
                           std::span<const BookLevel> asks) {
  int64_t reference = 1;
  if (!bids.empty()) {
    for (const auto& level : bids)
      reference = std::max(reference, level.price_ticks.value);
  } else {
    reference = std::numeric_limits<int64_t>::max();
    for (const auto& level : asks) {
      reference = std::min(reference, level.price_ticks.value);
    }
    if (reference == std::numeric_limits<int64_t>::max()) reference = 1;
  }
  const int64_t half = static_cast<int64_t>(dense_span_ / 2);
  dense_base_ = reference > half ? reference - half : 1;
}

void OrderBook::ApplyLevels(Side side, std::span<const BookLevel> levels) {
  auto& dense = side == Side::Buy ? dense_bids_ : dense_asks_;
  auto& overflow = side == Side::Buy ? overflow_bids_ : overflow_asks_;
  for (const auto& level : levels) {
    const int64_t price = level.price_ticks.value;
    const uint64_t quantity = level.quantity_lots.value;
    if (price >= dense_base_ &&
        static_cast<uint64_t>(price - dense_base_) < dense_span_) {
      dense[static_cast<size_t>(price - dense_base_)] = quantity;
      overflow.erase(price);
    } else if (quantity == 0) {
      overflow.erase(price);
    } else {
      overflow[price] = quantity;
    }
  }
}

void OrderBook::ApplySnapshot(std::span<const BookLevel> bids,
                              std::span<const BookLevel> asks) {
  Reset();
  InitWindow(bids, asks);
  ApplyLevels(Side::Buy, bids);
  ApplyLevels(Side::Sell, asks);
}

void OrderBook::ApplyDiff(std::span<const BookLevel> bids,
                          std::span<const BookLevel> asks) {
  ApplyLevels(Side::Buy, bids);
  ApplyLevels(Side::Sell, asks);
  RecenterIfNeeded();
}

void OrderBook::RecenterIfNeeded() {
  const auto bid = BestRaw(Side::Buy);
  const auto ask = BestRaw(Side::Sell);
  if (!bid && !ask) return;
  int64_t reference = bid ? bid->price_ticks.value : ask->price_ticks.value;
  if (bid && ask && bid->price_ticks.value < ask->price_ticks.value) {
    reference = bid->price_ticks.value +
                (ask->price_ticks.value - bid->price_ticks.value) / 2;
  }
  const uint64_t distance = reference >= dense_base_
                                ? static_cast<uint64_t>(reference - dense_base_)
                                : std::numeric_limits<uint64_t>::max();
  if (distance >= dense_span_ / 4 && distance < dense_span_ * 3 / 4) return;

  const auto move_dense_out =
      [&](const std::vector<uint64_t>& dense,
          absl::btree_map<int64_t, uint64_t>& overflow) {
        for (size_t index = 0; index < dense.size(); ++index) {
          if (dense[index] == 0) continue;
          const uint64_t price = static_cast<uint64_t>(dense_base_) + index;
          if (price <=
              static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            overflow[static_cast<int64_t>(price)] = dense[index];
          }
        }
      };
  move_dense_out(dense_bids_, overflow_bids_);
  move_dense_out(dense_asks_, overflow_asks_);
  std::fill(dense_bids_.begin(), dense_bids_.end(), 0);
  std::fill(dense_asks_.begin(), dense_asks_.end(), 0);
  const int64_t half = static_cast<int64_t>(dense_span_ / 2);
  dense_base_ = reference > half ? reference - half : 1;

  const auto move_inside = [&](absl::btree_map<int64_t, uint64_t>& overflow,
                               std::vector<uint64_t>& dense) {
    auto it = overflow.lower_bound(dense_base_);
    while (it != overflow.end() &&
           static_cast<uint64_t>(it->first - dense_base_) < dense_span_) {
      dense[static_cast<size_t>(it->first - dense_base_)] = it->second;
      it = overflow.erase(it);
    }
  };
  move_inside(overflow_bids_, dense_bids_);
  move_inside(overflow_asks_, dense_asks_);
}

void OrderBook::SetMetadata(BookSyncState state,
                            std::optional<uint64_t> sequence) {
  state_ = state;
  sequence_ = sequence;
}

size_t OrderBook::CopyTopNRaw(Side side, std::span<BookLevel> output) const {
  if (output.empty()) return 0;
  size_t count = 0;
  const auto consider = [&](int64_t price, uint64_t quantity) {
    if (quantity == 0) return;
    size_t index = 0;
    while (index < count &&
           (side == Side::Buy ? output[index].price_ticks.value > price
                              : output[index].price_ticks.value < price)) {
      ++index;
    }
    if (index >= output.size()) return;
    if (count < output.size()) ++count;
    for (size_t i = count - 1; i > index; --i) output[i] = output[i - 1];
    output[index] = BookLevel{PriceTicks{price}, QuantityLots{quantity}};
  };
  const auto& dense = side == Side::Buy ? dense_bids_ : dense_asks_;
  for (size_t index = 0; index < dense.size(); ++index) {
    if (dense[index] == 0) continue;
    const uint64_t price = static_cast<uint64_t>(dense_base_) + index;
    if (price <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      consider(static_cast<int64_t>(price), dense[index]);
    }
  }
  const auto& overflow = side == Side::Buy ? overflow_bids_ : overflow_asks_;
  for (const auto& [price, quantity] : overflow) consider(price, quantity);
  return count;
}

std::optional<BookLevel> OrderBook::BestRaw(Side side) const {
  std::array<BookLevel, 1> top{};
  if (CopyTopNRaw(side, top) == 0) return std::nullopt;
  return top[0];
}

bool OrderBook::IsCrossed() const {
  const auto bid = BestRaw(Side::Buy);
  const auto ask = BestRaw(Side::Sell);
  return bid && ask && bid->price_ticks.value >= ask->price_ticks.value;
}

std::optional<BookLevel> OrderBook::BestBid() const {
  if (state_ != BookSyncState::Live && state_ != BookSyncState::Stale)
    return std::nullopt;
  return BestRaw(Side::Buy);
}

std::optional<BookLevel> OrderBook::BestAsk() const {
  if (state_ != BookSyncState::Live && state_ != BookSyncState::Stale)
    return std::nullopt;
  return BestRaw(Side::Sell);
}

size_t OrderBook::CopyTopN(Side side, std::span<BookLevel> output) const {
  if (state_ != BookSyncState::Live && state_ != BookSyncState::Stale) return 0;
  return CopyTopNRaw(side, output);
}

}  // namespace hquant
