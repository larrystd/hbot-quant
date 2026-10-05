#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <vector>

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

 private:
  static bool ValidLevels(std::span<const BookLevel> levels, bool allow_zero);
  static void Apply(std::map<uint64_t, uint64_t>& side,
                    std::span<const BookLevel> levels);

  std::map<uint64_t, uint64_t> bids_;
  std::map<uint64_t, uint64_t> asks_;
};

}  // namespace hquant::v1
