#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace hquant::v1 {

inline constexpr uint16_t kMaxShards = 1000;

struct ShardId {
  uint16_t value = 0;
  bool operator==(const ShardId&) const = default;
};

struct MarketId {
  std::string symbol;
  bool operator==(const MarketId&) const = default;
};

struct OrderId {
  uint64_t value = 0;
  bool operator==(const OrderId&) const = default;
};

using MonoTime = std::chrono::time_point<std::chrono::steady_clock,
                                         std::chrono::microseconds>;

struct InputTime {
  int64_t at_us = 0;
  uint64_t ordinal = 0;
  bool operator==(const InputTime&) const = default;
};

}  // namespace hquant::v1
