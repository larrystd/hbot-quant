#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace hquant::bench {

enum class Side { Buy, Sell };
enum class Owner { External, Account };

struct SimOrder {
  uint64_t id = 0;
  std::string symbol;
  Side side = Side::Buy;
  Owner owner = Owner::External;
  int64_t price_nanos = 0;
  int64_t quantity_nanos = 0;
  int64_t remaining_nanos = 0;
  int64_t filled_nanos = 0;
  bool canceled = false;
};

struct SimFill {
  uint64_t maker_id = 0;
  uint64_t taker_id = 0;
  std::string symbol;
  int64_t price_nanos = 0;
  int64_t quantity_nanos = 0;
  Owner maker_owner = Owner::External;
  Owner taker_owner = Owner::External;
};

struct PlaceResult {
  uint64_t order_id = 0;
  int64_t remaining_nanos = 0;
  std::vector<SimFill> fills;
};

struct SimLevel {
  int64_t price_nanos = 0;
  int64_t quantity_nanos = 0;
};

// Single-owner matching core. All public book/trade messages and private fills
// must be emitted from the results of these same mutations.
class MatchingEngine {
 public:
  PlaceResult Place(std::string symbol, Side side, Owner owner,
                    int64_t price_nanos, int64_t quantity_nanos);
  // Returns the unfilled quantity released by a successful cancellation.
  std::optional<int64_t> Cancel(uint64_t order_id);
  const SimOrder* Find(uint64_t order_id) const;
  std::vector<SimLevel> Levels(const std::string& symbol, Side side) const;

 private:
  struct Book {
    std::map<int64_t, std::deque<uint64_t>, std::greater<int64_t>> bids;
    std::map<int64_t, std::deque<uint64_t>> asks;
  };

  template <typename LevelsMap>
  void Match(SimOrder& taker, LevelsMap& opposite,
             std::vector<SimFill>& fills);

  uint64_t next_id_ = 1;
  std::unordered_map<std::string, Book> books_;
  std::map<uint64_t, SimOrder> orders_;
};

}  // namespace hquant::bench
