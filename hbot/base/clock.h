#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

namespace hbot {

using UtcTime = std::chrono::sys_time<std::chrono::microseconds>;
using MonoTime = std::chrono::time_point<std::chrono::steady_clock,
                                         std::chrono::microseconds>;

struct EventTime {
  std::optional<UtcTime> exchange_utc;
  UtcTime receive_utc{};
  MonoTime receive_mono{};
};

struct InputStamp {
  int64_t at_us = 0;
  uint64_t ordinal = 0;
  bool operator==(const InputStamp&) const = default;
};

class Clock {
 public:
  virtual ~Clock() = default;
  virtual UtcTime UtcNow() const = 0;
  virtual MonoTime MonoNow() const = 0;
};

}  // namespace hbot
