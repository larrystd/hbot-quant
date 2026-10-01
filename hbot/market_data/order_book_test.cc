#include "hbot/market_data/order_book.h"

#include <array>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <vector>

namespace {

hbot::BookLevel Level(int64_t price, uint64_t quantity) {
  return {{price}, {quantity}};
}

void Require(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << '\n';
    std::exit(1);
  }
}

bool Same(const hbot::BookLevel& left, const hbot::BookLevel& right) {
  return left.price_ticks == right.price_ticks &&
         left.quantity_lots == right.quantity_lots;
}

bool Same(const std::optional<hbot::BookLevel>& left,
          const hbot::BookLevel& right) {
  return left && Same(*left, right);
}

}  // namespace

int main() {
  hbot::OrderBook book(8);
  const std::vector bids{Level(100, 10), Level(90, 20)};
  const std::vector asks{Level(110, 5), Level(200, 6)};
  book.ApplySnapshot(bids, asks);
  book.SetMetadata(hbot::BookSyncState::Live, 1);
  Require(Same(book.BestBid(), Level(100, 10)), "dense best bid");
  Require(Same(book.BestAsk(), Level(110, 5)), "overflow best ask");
  std::array<hbot::BookLevel, 3> top{};
  Require(book.CopyTopN(hbot::Side::Buy, top) == 2, "bid depth count");
  Require(Same(top[0], Level(100, 10)) && Same(top[1], Level(90, 20)),
          "bid depth order");
  Require(book.CopyTopN(hbot::Side::Sell, top) == 2, "ask depth count");
  Require(Same(top[0], Level(110, 5)) && Same(top[1], Level(200, 6)),
          "ask depth order");

  const std::vector changed_bids{Level(100, 0), Level(95, 7)};
  const std::vector changed_asks{Level(110, 0), Level(105, 9)};
  book.ApplyDiff(changed_bids, changed_asks);
  Require(Same(book.BestBid(), Level(95, 7)), "recentered best bid");
  Require(Same(book.BestAsk(), Level(105, 9)), "recentered best ask");
  Require(!book.IsCrossed(), "valid spread");

  const std::vector crossed{Level(106, 1)};
  book.ApplyDiff(crossed, {});
  Require(book.IsCrossed(), "crossed spread detected");
  book.SetMetadata(hbot::BookSyncState::Resyncing, 2);
  Require(!book.BestBid() && book.CopyTopN(hbot::Side::Buy, top) == 0,
          "unsafe book hidden");
  book.Reset();
  Require(!book.BestAsk(), "reset clears book");
  return 0;
}
