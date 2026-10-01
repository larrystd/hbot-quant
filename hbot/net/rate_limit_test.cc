#include "hbot/net/rate_limit.h"

#include <array>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace hbot {
namespace {

TEST(RateLimitTest, ReservesCancellationSlotsAndResetsWindow) {
  GlobalRateBreaker breaker;
  RateLimiter limiter(&breaker);
  RateLimitKey key{"paper", "127.0.0.1", "/orders"};
  ASSERT_TRUE(limiter.Configure(key, RateLease{3, 1, std::chrono::seconds(1)}));
  auto now = RateLimiter::Clock::now();
  EXPECT_EQ(limiter.TryAcquire(key, false, now), RateDecision::Granted);
  EXPECT_EQ(limiter.TryAcquire(key, false, now), RateDecision::Granted);
  EXPECT_EQ(limiter.TryAcquire(key, false, now), RateDecision::Exhausted);
  EXPECT_EQ(limiter.TryAcquire(key, true, now), RateDecision::Granted);
  EXPECT_EQ(limiter.TryAcquire(key, true, now), RateDecision::Exhausted);
  EXPECT_EQ(limiter.TryAcquire(key, false, now + std::chrono::seconds(1)),
            RateDecision::Granted);
}

TEST(RateLimitTest, HttpThrottleTripsSharedBreaker) {
  GlobalRateBreaker breaker;
  RateLimiter a(&breaker), b(&breaker);
  RateLimitKey key{"paper", "127.0.0.1", "/orders"};
  ASSERT_TRUE(a.Configure(key, RateLease{2, 1, std::chrono::seconds(1)}));
  ASSERT_TRUE(b.Configure(key, RateLease{2, 1, std::chrono::seconds(1)}));
  auto now = RateLimiter::Clock::now();
  a.ObserveHttpStatus(429, now, std::chrono::seconds(5));
  EXPECT_EQ(b.TryAcquire(key, true, now), RateDecision::BreakerOpen);
  EXPECT_EQ(b.TryAcquire(key, false, now + std::chrono::seconds(5)),
            RateDecision::Granted);
  b.ObserveHttpStatus(418, now, std::chrono::seconds(1));
  EXPECT_EQ(a.TryAcquire(key, false, now + std::chrono::seconds(30)),
            RateDecision::BreakerOpen);
}

TEST(RateLimitTest, RejectsOversubscribedAndMalformedStaticGrants) {
  using namespace std::chrono_literals;
  RateLimitKey key{"account", "127.0.0.1", "REQUEST_WEIGHT/1m"};
  std::vector<RateCapacity> capacities{{key, 10, 1min}};
  std::vector<RateLeaseGrant> grants{
      {key, ShardId{0}, 1, RateLease{6, 1, 1min, 1}},
      {key, ShardId{1}, 1, RateLease{4, 1, 1min, 1}}};
  EXPECT_TRUE(ValidateStaticRateLeases(capacities, grants).ok());
  grants[1].lease.limit = 5;
  EXPECT_EQ(ValidateStaticRateLeases(capacities, grants).code(),
            absl::StatusCode::kResourceExhausted);
  grants[1].lease.limit = 4;
  grants[1].shard = ShardId{0};
  EXPECT_EQ(ValidateStaticRateLeases(capacities, grants).code(),
            absl::StatusCode::kAlreadyExists);
  grants[1].shard = ShardId{8};
  EXPECT_EQ(ValidateStaticRateLeases(capacities, grants).code(),
            absl::StatusCode::kInvalidArgument);
  grants[1].shard = ShardId{1};
  grants[1].lease.window = 10s;
  EXPECT_EQ(ValidateStaticRateLeases(capacities, grants).code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST(RateLimitTest, WeightedPrioritiesProtectOrderAndCancellationCapacity) {
  using namespace std::chrono_literals;
  GlobalRateBreaker breaker;
  RateLimitKey key{"account", "127.0.0.1", "REQUEST_WEIGHT/1m"};
  RateLimiter limiter(ShardId{0}, &breaker);
  RateLeaseGrant grant{key, ShardId{0}, 1, RateLease{10, 2, 1min, 3}};
  ASSERT_TRUE(limiter.InstallGrant(grant).ok());
  EXPECT_EQ(limiter.InstallGrant(grant).code(),
            absl::StatusCode::kAlreadyExists);
  RateLimiter wrong(ShardId{1}, &breaker);
  EXPECT_EQ(wrong.InstallGrant(grant).code(),
            absl::StatusCode::kInvalidArgument);
  breaker.Seal();
  const auto now = RateLimiter::Clock::now();
  EXPECT_EQ(limiter.TryAcquire(key, RatePriority::Background, 0, now),
            RateDecision::InvalidWeight);
  EXPECT_EQ(limiter.TryAcquire(key, RatePriority::Background, 5, now),
            RateDecision::Granted);
  EXPECT_EQ(limiter.TryAcquire(key, RatePriority::Background, 1, now),
            RateDecision::Exhausted);
  EXPECT_EQ(limiter.TryAcquire(key, RatePriority::Order, 3, now),
            RateDecision::Granted);
  EXPECT_EQ(limiter.TryAcquire(key, RatePriority::Order, 1, now),
            RateDecision::Exhausted);
  EXPECT_EQ(limiter.TryAcquire(key, RatePriority::Cancel, 2, now),
            RateDecision::Granted);
  EXPECT_EQ(limiter.TryAcquire(key, RatePriority::Cancel, 1, now),
            RateDecision::Exhausted);
}

TEST(RateLimitTest, Keyed429StopsAllShardsForBucketOnly) {
  using namespace std::chrono_literals;
  GlobalRateBreaker breaker;
  RateLimitKey orders{"account", "127.0.0.1", "ORDERS/10s"};
  RateLimitKey weight{"account", "127.0.0.1", "REQUEST_WEIGHT/1m"};
  RateLimiter a(ShardId{0}, &breaker), b(ShardId{1}, &breaker);
  ASSERT_TRUE(a.InstallGrant({orders, ShardId{0}, 1, {3, 1, 10s}}).ok());
  ASSERT_TRUE(b.InstallGrant({orders, ShardId{1}, 1, {3, 1, 10s}}).ok());
  ASSERT_TRUE(b.InstallGrant({weight, ShardId{1}, 1, {3, 1, 1min}}).ok());
  breaker.Seal();
  const auto now = RateLimiter::Clock::now();
  a.ObserveHttpStatus(orders, 429, now, 5s);
  EXPECT_EQ(b.TryAcquire(orders, RatePriority::Cancel, 1, now),
            RateDecision::BreakerOpen);
  EXPECT_EQ(b.TryAcquire(weight, RatePriority::Order, 1, now),
            RateDecision::Granted);
  EXPECT_EQ(b.TryAcquire(orders, RatePriority::Order, 1, now + 5s),
            RateDecision::Granted);
  EXPECT_FALSE(breaker.RegisterKey({"account", "127.0.0.1", "new"}));
  EXPECT_TRUE(breaker.IsOpen({"account", "127.0.0.1", "new"}, now));
}

TEST(RateLimitTest, EightShardQuotasNeverExceedGlobalLease) {
  using namespace std::chrono_literals;
  RateLimitKey key{"account", "127.0.0.1", "ORDERS/10s"};
  std::vector<RateCapacity> capacities{{key, 80, 10s}};
  std::vector<RateLeaseGrant> grants;
  GlobalRateBreaker breaker;
  std::array<std::unique_ptr<RateLimiter>, 8> limiters;
  for (uint8_t shard = 0; shard < 8; ++shard) {
    grants.push_back({key, ShardId{shard}, 1, {10, 2, 10s}});
    limiters[shard] = std::make_unique<RateLimiter>(ShardId{shard}, &breaker);
    ASSERT_TRUE(limiters[shard]->InstallGrant(grants.back()).ok());
  }
  ASSERT_TRUE(ValidateStaticRateLeases(capacities, grants).ok());
  breaker.Seal();
  const auto now = RateLimiter::Clock::now();
  std::array<unsigned, 8> granted{};
  std::vector<std::thread> threads;
  for (unsigned shard = 0; shard < 8; ++shard) {
    threads.emplace_back([&, shard] {
      for (unsigned i = 0; i < 20; ++i) {
        if (limiters[shard]->TryAcquire(key, RatePriority::Cancel, 1, now) ==
            RateDecision::Granted)
          ++granted[shard];
      }
    });
  }
  for (auto& thread : threads) thread.join();
  unsigned total = 0;
  for (unsigned count : granted) total += count;
  EXPECT_EQ(total, 80);
}

}  // namespace
}  // namespace hbot
