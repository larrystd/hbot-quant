#pragma once

#include <cstddef>
#include <optional>
#include <span>

#include "hbot/market_data/book_sync.h"
#include "hbot/model/book_event.h"

namespace hbot {

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

}  // namespace hbot
