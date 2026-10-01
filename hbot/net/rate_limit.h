#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include "absl/status/status.h"
#include "hbot/base/ids.h"

namespace hbot {

// One local lease applies to one account/IP/endpoint key. The owner io_context
// serializes calls to RateLimiter; only the breaker is shared across shards.
struct RateLimitKey {
  std::string account;
  std::string ip;
  std::string endpoint;

  auto operator<=>(const RateLimitKey&) const = default;
};

struct RateLease {
  uint32_t limit = 0;
  uint32_t cancel_reserve = 0;
  std::chrono::steady_clock::duration window{};
  uint32_t order_reserve = 0;
};

struct RateCapacity {
  RateLimitKey key;
  uint32_t global_limit = 0;
  std::chrono::steady_clock::duration window{};
};

struct RateLeaseGrant {
  RateLimitKey key;
  ShardId shard;
  uint64_t version = 0;
  RateLease lease;
};

// Control-thread startup check. A key is one exchange-enforced bucket (e.g.
// IP REQUEST_WEIGHT/1m or account ORDERS/10s), normalized identically across
// all shards. No runtime borrowing or reassignment is permitted in v1.
absl::Status ValidateStaticRateLeases(std::span<const RateCapacity> capacities,
                                      std::span<const RateLeaseGrant> grants);

enum class RatePriority { Background, Order, Cancel };
enum class RateDecision {
  Granted,
  NoLease,
  Exhausted,
  BreakerOpen,
  InvalidWeight
};

class GlobalRateBreaker {
 public:
  using Clock = std::chrono::steady_clock;

  void Trip(Clock::time_point until);
  bool IsOpen(Clock::time_point now) const;
  // Register every key before shard threads start, then Seal. Keyed trips
  // affect only the exchange bucket that returned 429/418.
  bool RegisterKey(const RateLimitKey& key);
  void Seal();
  void Trip(const RateLimitKey& key, Clock::time_point until);
  bool IsOpen(const RateLimitKey& key, Clock::time_point now) const;

 private:
  std::atomic<int64_t> open_until_ns_{0};
  std::map<RateLimitKey, std::unique_ptr<std::atomic<int64_t>>> keyed_until_ns_;
  std::atomic<bool> sealed_{false};
};

class RateLimiter {
 public:
  using Clock = std::chrono::steady_clock;

  explicit RateLimiter(GlobalRateBreaker* breaker = nullptr)
      : breaker_(breaker) {}
  RateLimiter(ShardId shard, GlobalRateBreaker* breaker)
      : breaker_(breaker), shard_(shard) {}

  bool Configure(RateLimitKey key, RateLease lease);
  absl::Status InstallGrant(const RateLeaseGrant& grant);
  RateDecision TryAcquire(const RateLimitKey& key, bool is_cancel,
                          Clock::time_point now);
  RateDecision TryAcquire(const RateLimitKey& key, RatePriority priority,
                          uint32_t weight, Clock::time_point now);
  void ObserveHttpStatus(unsigned status, Clock::time_point now,
                         Clock::duration retry_after);
  void ObserveHttpStatus(const RateLimitKey& key, unsigned status,
                         Clock::time_point now, Clock::duration retry_after);

 private:
  struct Window {
    RateLease lease;
    Clock::time_point starts_at{};
    uint32_t used = 0;
    bool started = false;
    uint64_t version = 0;
  };

  GlobalRateBreaker* breaker_;
  std::optional<ShardId> shard_;
  std::map<RateLimitKey, Window> windows_;
};

}  // namespace hbot
