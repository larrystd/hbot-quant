#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/statusor.h"
#include "mpdecimal.h"

namespace hquant {

enum class RoundingMode { Down, Up, Floor, Ceiling, HalfEven, HalfUp };

// Immutable 28-significant-digit value. Monetary operations never pass through
// binary floating point. Quantization requires an explicit rounding mode.
class Decimal {
 public:
  Decimal();
  static absl::StatusOr<Decimal> Parse(std::string_view text);

  std::string ToString() const;
  bool IsStrictlyPositive() const;
  bool IsNonnegative() const;
  absl::StatusOr<Decimal> Add(const Decimal& other) const;
  absl::StatusOr<Decimal> Subtract(const Decimal& other) const;
  absl::StatusOr<Decimal> Multiply(const Decimal& other) const;
  absl::StatusOr<Decimal> Divide(const Decimal& other) const;
  absl::StatusOr<Decimal> Quantize(const Decimal& quantum,
                                   RoundingMode rounding) const;
  absl::StatusOr<int> Compare(const Decimal& other) const;

 private:
  struct Deleter {
    void operator()(mpd_t* value) const;
  };
  using Value = std::shared_ptr<mpd_t>;
  explicit Decimal(Value value) : value_(std::move(value)) {}
  static Value NewValue();
  Value value_;
};

template <typename Tag>
struct StringId {
  std::string value;

  StringId() = default;
  explicit StringId(std::string text) : value(std::move(text)) {}
  bool operator==(const StringId&) const = default;

  template <typename H>
  friend H AbslHashValue(H hash, const StringId& id) {
    return H::combine(std::move(hash), id.value);
  }
};

struct VenueTag {};
struct AccountTag {};
struct AssetTag {};
struct StrategyTag {};
struct ExecutorTag {};
struct ClientOrderTag {};
struct ExchangeOrderTag {};
struct ExchangeTradeTag {};

using VenueId = StringId<VenueTag>;
using AccountId = StringId<AccountTag>;
using AssetId = StringId<AssetTag>;
using StrategyId = StringId<StrategyTag>;
using ExecutorId = StringId<ExecutorTag>;
using ClientOrderId = StringId<ClientOrderTag>;
using ExchangeOrderId = StringId<ExchangeOrderTag>;
using ExchangeTradeId = StringId<ExchangeTradeTag>;

struct OwnerId {
  // Explicit 48-bit positive key persisted in config and the client order ID.
  uint64_t owner_key = 0;
  StrategyId strategy_id;
  std::optional<ExecutorId> executor_id;
  bool IsValid() const {
    return owner_key > 0 && owner_key < (uint64_t{1} << 48) &&
           !strategy_id.value.empty() &&
           (!executor_id || !executor_id->value.empty());
  }
  bool operator==(const OwnerId&) const = default;
};

struct RunId {
  uint64_t value = 0;
  bool IsValid() const { return value != 0; }
  bool operator==(const RunId&) const = default;
};
struct ShardId {
  uint8_t value = 0;
  bool IsValid() const { return value < 8; }
  bool operator==(const ShardId&) const = default;
};
struct ReservationId {
  uint64_t value = 0;
  bool operator==(const ReservationId&) const = default;
};
struct DecisionId {
  uint64_t value = 0;
  bool operator==(const DecisionId&) const = default;
};

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

}  // namespace hquant
