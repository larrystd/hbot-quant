#include "hquant/shard/order_book.h"

namespace hquant::v1 {

void OrderBook::Reset() {
  bids_.clear();
  asks_.clear();
}

bool OrderBook::ValidLevels(std::span<const BookLevel> levels,
                            bool allow_zero) {
  for (const BookLevel& level : levels) {
    if (level.price_ticks == 0 ||
        (!allow_zero && level.quantity_lots == 0)) {
      return false;
    }
  }
  return true;
}

void OrderBook::Apply(absl::btree_map<uint64_t, uint64_t>& side,
                      std::span<const BookLevel> levels) {
  for (const BookLevel& level : levels) {
    if (level.quantity_lots == 0) {
      side.erase(level.price_ticks);
    } else {
      side[level.price_ticks] = level.quantity_lots;
    }
  }
}

bool OrderBook::ApplySnapshot(std::span<const BookLevel> bids,
                              std::span<const BookLevel> asks) {
  if (!ValidLevels(bids, false) || !ValidLevels(asks, false)) return false;
  Reset();
  Apply(bids_, bids);
  Apply(asks_, asks);
  return IsTradable();
}

bool OrderBook::ApplyDiff(std::span<const BookLevel> bids,
                          std::span<const BookLevel> asks) {
  if (!ValidLevels(bids, true) || !ValidLevels(asks, true)) return false;
  Apply(bids_, bids);
  Apply(asks_, asks);
  return IsTradable();
}

bool OrderBook::IsTradable() const {
  return !bids_.empty() && !asks_.empty() && bids_.rbegin()->first < asks_.begin()->first;
}

std::optional<BookLevel> OrderBook::BestBid() const {
  if (bids_.empty()) return std::nullopt;
  const auto& [price, quantity] = *bids_.rbegin();
  return BookLevel{price, quantity};
}

std::optional<BookLevel> OrderBook::BestAsk() const {
  if (asks_.empty()) return std::nullopt;
  const auto& [price, quantity] = *asks_.begin();
  return BookLevel{price, quantity};
}

std::vector<BookLevel> OrderBook::Bids() const {
  std::vector<BookLevel> levels;
  levels.reserve(bids_.size());
  for (auto it = bids_.rbegin(); it != bids_.rend(); ++it) {
    levels.push_back(BookLevel{it->first, it->second});
  }
  return levels;
}

std::vector<BookLevel> OrderBook::Asks() const {
  std::vector<BookLevel> levels;
  levels.reserve(asks_.size());
  for (const auto& [price, quantity] : asks_) {
    levels.push_back(BookLevel{price, quantity});
  }
  return levels;
}

}  // namespace hquant::v1
