#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "hquant/base/ids.h"
#include "hquant/shard/order_book.h"

namespace hquant::v1 {

struct FeedConnected {
  uint64_t connection_epoch = 0;
};

struct FeedDisconnected {
  uint64_t connection_epoch = 0;
  std::string reason;
};

struct BookSnapshot {
  MarketId market;
  uint64_t connection_epoch = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
};

struct BookDiff {
  MarketId market;
  uint64_t connection_epoch = 0;
  uint64_t first_sequence = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
};

enum class Side { Buy, Sell };

struct PublicTrade {
  MarketId market;
  uint64_t connection_epoch = 0;
  uint64_t trade_id = 0;
  uint64_t price_ticks = 0;
  uint64_t quantity_lots = 0;
  Side aggressor = Side::Buy;
};

using MarketInput = std::variant<FeedConnected, BookSnapshot, BookDiff,
                                 PublicTrade, FeedDisconnected>;

enum class BookState { Syncing, Live };

struct BookView {
  BookState state = BookState::Syncing;
  std::optional<uint64_t> last_sequence;
  std::optional<BookLevel> best_bid;
  std::optional<BookLevel> best_ask;
  std::optional<BookLevel> last_trade;
  std::optional<MonoTime> last_book_at;
};

struct MarketNotice {
  enum class Kind { BookChanged, PublicTrade, BookUnavailable } kind;
  std::optional<hquant::v1::PublicTrade> trade;
};

}  // namespace hquant::v1
