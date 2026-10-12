#include "hquant/bench/venue/matching_engine.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace hquant::bench {

template <typename LevelsMap>
void MatchingEngine::Match(SimOrder& taker, LevelsMap& opposite,
                           std::vector<SimFill>& fills) {
  while (taker.remaining_nanos > 0 && !opposite.empty()) {
    auto level = opposite.begin();
    const bool crosses = taker.side == Side::Buy
                             ? taker.price_nanos >= level->first
                             : taker.price_nanos <= level->first;
    if (!crosses) break;
    auto& queue = level->second;
    while (taker.remaining_nanos > 0 && !queue.empty()) {
      SimOrder& maker = orders_.at(queue.front());
      if (maker.remaining_nanos == 0) {
        queue.pop_front();
        continue;
      }
      const int64_t quantity =
          std::min(taker.remaining_nanos, maker.remaining_nanos);
      fills.push_back(SimFill{maker.id, taker.id, taker.symbol,
                              maker.price_nanos, quantity, maker.owner,
                              taker.owner});
      maker.remaining_nanos -= quantity;
      maker.filled_nanos += quantity;
      taker.remaining_nanos -= quantity;
      taker.filled_nanos += quantity;
      if (maker.remaining_nanos == 0) queue.pop_front();
    }
    if (queue.empty()) opposite.erase(level);
  }
}

PlaceResult MatchingEngine::Place(std::string symbol, Side side, Owner owner,
                                  int64_t price_nanos,
                                  int64_t quantity_nanos) {
  if (symbol.empty() || price_nanos <= 0 || quantity_nanos <= 0) {
    throw std::invalid_argument("symbol, price and quantity must be positive");
  }
  const uint64_t id = next_id_++;
  auto [it, inserted] = orders_.emplace(
      id, SimOrder{id, std::move(symbol), side, owner, price_nanos,
                   quantity_nanos, quantity_nanos, 0, false});
  (void)inserted;
  SimOrder& taker = it->second;
  Book& book = books_[taker.symbol];
  PlaceResult result;
  result.order_id = id;
  if (side == Side::Buy) {
    Match(taker, book.asks, result.fills);
    if (taker.remaining_nanos > 0) book.bids[price_nanos].push_back(id);
  } else {
    Match(taker, book.bids, result.fills);
    if (taker.remaining_nanos > 0) book.asks[price_nanos].push_back(id);
  }
  result.remaining_nanos = taker.remaining_nanos;
  return result;
}

std::optional<int64_t> MatchingEngine::Cancel(uint64_t order_id) {
  auto it = orders_.find(order_id);
  if (it == orders_.end() || it->second.remaining_nanos == 0 ||
      it->second.canceled) {
    return std::nullopt;
  }
  const int64_t released = it->second.remaining_nanos;
  it->second.remaining_nanos = 0;
  it->second.canceled = true;
  return released;
}

const SimOrder* MatchingEngine::Find(uint64_t order_id) const {
  auto it = orders_.find(order_id);
  return it == orders_.end() ? nullptr : &it->second;
}

std::vector<SimLevel> MatchingEngine::Levels(const std::string& symbol,
                                             Side side) const {
  std::vector<SimLevel> result;
  auto book_it = books_.find(symbol);
  if (book_it == books_.end()) return result;
  auto collect = [&](const auto& levels) {
    for (const auto& [price, queue] : levels) {
      int64_t quantity = 0;
      for (uint64_t id : queue) quantity += orders_.at(id).remaining_nanos;
      if (quantity > 0) result.push_back({price, quantity});
    }
  };
  if (side == Side::Buy)
    collect(book_it->second.bids);
  else
    collect(book_it->second.asks);
  return result;
}

}  // namespace hquant::bench
