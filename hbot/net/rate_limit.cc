#include "hbot/net/rate_limit.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <utility>

namespace hbot {
namespace {

using Clock = std::chrono::steady_clock;

int64_t Nanos(Clock::time_point value) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             value.time_since_epoch())
      .count();
}

bool ValidKey(const RateLimitKey& key) {
  return !key.endpoint.empty() && (!key.account.empty() || !key.ip.empty());
}

bool ValidLease(const RateLease& lease) {
  return lease.limit > 0 && lease.window > Clock::duration::zero() &&
         lease.cancel_reserve <= lease.limit &&
         lease.order_reserve <= lease.limit - lease.cancel_reserve;
}

Clock::duration RetryDelay(unsigned status, Clock::duration retry_after) {
  const auto minimum =
      status == 418 ? std::chrono::minutes(1) : std::chrono::seconds(1);
  return std::max(retry_after, Clock::duration(minimum));
}

void Extend(std::atomic<int64_t>& until, int64_t requested) {
  int64_t prior = until.load(std::memory_order_relaxed);
  while (prior < requested && !until.compare_exchange_weak(
                                  prior, requested, std::memory_order_release,
                                  std::memory_order_relaxed)) {
  }
}

}  // namespace

absl::Status ValidateStaticRateLeases(std::span<const RateCapacity> capacities,
                                      std::span<const RateLeaseGrant> grants) {
  std::map<RateLimitKey, RateCapacity> by_key;
  for (const auto& capacity : capacities) {
    if (!ValidKey(capacity.key) || capacity.global_limit == 0 ||
        capacity.window <= Clock::duration::zero()) {
      return absl::InvalidArgumentError("invalid global rate capacity");
    }
    if (!by_key.emplace(capacity.key, capacity).second) {
      return absl::AlreadyExistsError("duplicate global rate capacity");
    }
  }
  std::map<RateLimitKey, uint64_t> total;
  std::set<std::pair<RateLimitKey, uint8_t>> assigned;
  for (const auto& grant : grants) {
    if (!grant.shard.IsValid() || grant.version == 0 || !ValidKey(grant.key) ||
        !ValidLease(grant.lease)) {
      return absl::InvalidArgumentError("invalid rate lease grant");
    }
    auto capacity = by_key.find(grant.key);
    if (capacity == by_key.end() ||
        capacity->second.window != grant.lease.window) {
      return absl::FailedPreconditionError(
          "rate lease has no matching global capacity/window");
    }
    if (!assigned.emplace(grant.key, grant.shard.value).second) {
      return absl::AlreadyExistsError("duplicate shard lease");
    }
    auto& sum = total[grant.key];
    sum += grant.lease.limit;
    if (sum > capacity->second.global_limit) {
      return absl::ResourceExhaustedError(
          "sum of static leases exceeds exchange capacity");
    }
  }
  return absl::OkStatus();
}

void GlobalRateBreaker::Trip(Clock::time_point until) {
  Extend(open_until_ns_, Nanos(until));
}

bool GlobalRateBreaker::IsOpen(Clock::time_point now) const {
  return Nanos(now) < open_until_ns_.load(std::memory_order_acquire);
}

bool GlobalRateBreaker::RegisterKey(const RateLimitKey& key) {
  if (!ValidKey(key)) return false;
  if (keyed_until_ns_.contains(key)) return true;
  if (sealed_.load(std::memory_order_acquire)) return false;
  keyed_until_ns_.emplace(key, std::make_unique<std::atomic<int64_t>>(0));
  return true;
}

void GlobalRateBreaker::Seal() {
  sealed_.store(true, std::memory_order_release);
}

void GlobalRateBreaker::Trip(const RateLimitKey& key, Clock::time_point until) {
  auto it = keyed_until_ns_.find(key);
  if (it == keyed_until_ns_.end()) {
    // An unregistered bucket is a configuration fault. Fail closed.
    Trip(until);
    return;
  }
  Extend(*it->second, Nanos(until));
}

bool GlobalRateBreaker::IsOpen(const RateLimitKey& key,
                               Clock::time_point now) const {
  if (IsOpen(now)) return true;
  auto it = keyed_until_ns_.find(key);
  if (it == keyed_until_ns_.end()) {
    return sealed_.load(std::memory_order_acquire);
  }
  return Nanos(now) < it->second->load(std::memory_order_acquire);
}

bool RateLimiter::Configure(RateLimitKey key, RateLease lease) {
  if (!ValidKey(key) || !ValidLease(lease) ||
      (breaker_ && !breaker_->RegisterKey(key))) {
    return false;
  }
  windows_[std::move(key)] = Window{lease};
  return true;
}

absl::Status RateLimiter::InstallGrant(const RateLeaseGrant& grant) {
  if (!shard_ || !shard_->IsValid() || grant.shard.value != shard_->value ||
      grant.version == 0 || !ValidKey(grant.key) || !ValidLease(grant.lease)) {
    return absl::InvalidArgumentError("invalid or mismatched shard lease");
  }
  if (windows_.contains(grant.key)) {
    return absl::AlreadyExistsError("lease already installed");
  }
  if (breaker_ && !breaker_->RegisterKey(grant.key)) {
    return absl::FailedPreconditionError("rate bucket not registered");
  }
  windows_.emplace(grant.key, Window{grant.lease, {}, 0, false, grant.version});
  return absl::OkStatus();
}

RateDecision RateLimiter::TryAcquire(const RateLimitKey& key, bool is_cancel,
                                     Clock::time_point now) {
  return TryAcquire(key, is_cancel ? RatePriority::Cancel : RatePriority::Order,
                    1, now);
}

RateDecision RateLimiter::TryAcquire(const RateLimitKey& key,
                                     RatePriority priority, uint32_t weight,
                                     Clock::time_point now) {
  if (weight == 0) return RateDecision::InvalidWeight;
  if (breaker_ && breaker_->IsOpen(key, now)) {
    return RateDecision::BreakerOpen;
  }
  auto it = windows_.find(key);
  if (it == windows_.end()) return RateDecision::NoLease;
  auto& w = it->second;
  if (!w.started || now < w.starts_at || now - w.starts_at >= w.lease.window) {
    w.starts_at = now;
    w.used = 0;
    w.started = true;
  }
  uint32_t ceiling = w.lease.limit;
  if (priority != RatePriority::Cancel) {
    ceiling -= w.lease.cancel_reserve;
  }
  if (priority == RatePriority::Background) {
    ceiling -= w.lease.order_reserve;
  }
  if (w.used >= ceiling || weight > ceiling - w.used) {
    return RateDecision::Exhausted;
  }
  w.used += weight;
  return RateDecision::Granted;
}

void RateLimiter::ObserveHttpStatus(unsigned status, Clock::time_point now,
                                    Clock::duration retry_after) {
  if (breaker_ && (status == 429 || status == 418)) {
    breaker_->Trip(now + RetryDelay(status, retry_after));
  }
}

void RateLimiter::ObserveHttpStatus(const RateLimitKey& key, unsigned status,
                                    Clock::time_point now,
                                    Clock::duration retry_after) {
  if (breaker_ && (status == 429 || status == 418)) {
    breaker_->Trip(key, now + RetryDelay(status, retry_after));
  }
}

}  // namespace hbot
