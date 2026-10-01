#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace hbot {

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

}  // namespace hbot
