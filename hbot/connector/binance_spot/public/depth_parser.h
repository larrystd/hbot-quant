#pragma once

#include <string_view>

#include "absl/status/statusor.h"
#include "hbot/model/book_event.h"

namespace hbot::binance_spot {

// Parses Binance Spot JSON only. Fixed BookScale is checked before any integer
// conversion; no floating-point price or quantity enters the book.
class DepthParser {
 public:
  DepthParser(MarketId market, BookScale scale)
      : market_(std::move(market)), scale_(std::move(scale)) {}

  absl::StatusOr<BookSnapshot> ParseSnapshot(std::string_view json,
                                             uint64_t stream_epoch,
                                             EventTime received) const;
  absl::StatusOr<BookDiff> ParseDiff(std::string_view json,
                                     uint64_t stream_epoch,
                                     EventTime received) const;
  absl::StatusOr<PublicTrade> ParseTrade(std::string_view json,
                                         EventTime received) const;

  const MarketId& Market() const { return market_; }
  const BookScale& Scale() const { return scale_; }

 private:
  MarketId market_;
  BookScale scale_;
};

}  // namespace hbot::binance_spot
