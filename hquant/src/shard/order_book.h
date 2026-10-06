#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "absl/container/btree_map.h"

namespace hquant::v1 {

struct BookLevel {
  uint64_t price_ticks = 0;
  uint64_t quantity_lots = 0;
  bool operator==(const BookLevel&) const = default;
};

class OrderBook {
 public:
  void Reset();
  bool ApplySnapshot(std::span<const BookLevel> bids,
                     std::span<const BookLevel> asks);
  bool ApplyDiff(std::span<const BookLevel> bids,
                 std::span<const BookLevel> asks);
  bool IsTradable() const;
  std::optional<BookLevel> BestBid() const;
  std::optional<BookLevel> BestAsk() const;
  std::vector<BookLevel> Bids() const;
  std::vector<BookLevel> Asks() const;

 private:
  static bool ValidLevels(std::span<const BookLevel> levels, bool allow_zero);
  static void Apply(absl::btree_map<uint64_t, uint64_t>& side,
                    std::span<const BookLevel> levels);

  // Price tick -> quantity lot. Best bid is the last bid; best ask is the
  // first ask.
  absl::btree_map<uint64_t, uint64_t> bids_;
  absl::btree_map<uint64_t, uint64_t> asks_;
};

}  // namespace hquant::v1
