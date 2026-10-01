#include "hbot/market_data/order_book.h"

#include <algorithm>
#include <array>
#include <limits>

namespace hbot {

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

}  // namespace hbot
