#include <array>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "base/rate_limit.h"
#include "gtest/gtest.h"
#include "order/risk.h"
#include "shard/routing.h"

namespace hquant {
namespace {

Decimal D(const char* text) { return *Decimal::Parse(text); }

TEST(MultiShardTest, CapitalRateAndAccountReportsStayWithinTheirShard) {
  const auto now = UtcTime(std::chrono::microseconds(1'000'000));
  const AccountId account("shared");
  const AssetId quote("USDT");
  const MarketId market{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
  const MarketSpec spec{market, AssetId("BTC"), quote};
  const TradingRule rule{market,    D("0.01"), D("0.001"), D("0.001"),
                         D("0.01"), {},        1,          now};
  const StrategyId owner0{1, StrategyName("simple_pmm")};
  const StrategyId owner1{2, StrategyName("simple_pmm")};

  RiskBudgetAllocator book;
  ASSERT_TRUE(book.SetConservativeLimit(account, quote, D("100")).ok());
  RiskBudget lease0{account, quote,   ShardId{0},
                    1,       D("60"), now + std::chrono::hours(1)};
  RiskBudget lease1{account, quote,   ShardId{1},
                    1,       D("40"), now + std::chrono::hours(1)};
  ASSERT_TRUE(book.GrantInitial(lease0, now).ok());
  ASSERT_TRUE(book.GrantInitial(lease1, now).ok());
  EXPECT_EQ(*book.GrantedTotal(account, quote)->Compare(D("100")), 0);
  EXPECT_FALSE(book.GrantInitial({account, quote, ShardId{2}, 1, D("1"),
                                  now + std::chrono::hours(1)},
                                 now)
                   .ok());
  RiskGate risk0({ShardId{0}, D("0"), std::chrono::seconds(300)});
  RiskGate risk1({ShardId{1}, D("0"), std::chrono::seconds(300)});
  ASSERT_TRUE(risk0.SetInitialBudget(lease0).ok());
  ASSERT_TRUE(risk1.SetInitialBudget(lease1).ok());
  SubmitOrder request;
  request.side = Side::Buy;
  request.quantity = D("1");
  request.price = D("30");
  auto first =
      risk0.TryHold(account, owner0, request, spec, rule, now, true, true);
  auto second =
      risk1.TryHold(account, owner1, request, spec, rule, now, true, true);
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_TRUE(second.ok()) << second.status();
  ASSERT_TRUE(risk0.MarkSubmissionUnknown(first->hold_id).ok());
  EXPECT_EQ(*risk0.Available(account, quote)->Compare(D("30")), 0);
  EXPECT_FALSE(
      risk1.TryHold(account, owner1, request, spec, rule, now, true, true)
          .ok());
  EXPECT_EQ(*risk1.Available(account, quote)->Compare(D("10")), 0);

  RateLimitKey rate_key{"shared", "127.0.0.1", "ORDERS/10s"};
  RateCapacity capacity{rate_key, 10, std::chrono::seconds(10)};
  std::array<RateBudgetAssignment, 2> grants{{
      {rate_key, ShardId{0}, 1, {6, 1, std::chrono::seconds(10), 1}},
      {rate_key, ShardId{1}, 1, {4, 1, std::chrono::seconds(10), 1}},
  }};
  ASSERT_TRUE(ValidateRateBudgets(std::span(&capacity, 1), grants).ok());
  GlobalRateBreaker breaker;
  ASSERT_TRUE(breaker.RegisterKey(rate_key));
  breaker.Seal();
  RateLimiter rate0(ShardId{0}, &breaker), rate1(ShardId{1}, &breaker);
  ASSERT_TRUE(rate0.InstallGrant(grants[0]).ok());
  ASSERT_TRUE(rate1.InstallGrant(grants[1]).ok());
  const auto mono = std::chrono::steady_clock::now();
  EXPECT_EQ(rate0.TryAcquire(rate_key, RatePriority::Order, 1, mono),
            ErrorCode::kOk);
  EXPECT_EQ(rate1.TryAcquire(rate_key, RatePriority::Order, 1, mono),
            ErrorCode::kOk);
  rate0.ObserveHttpStatus(rate_key, 429, mono, std::chrono::seconds(1));
  EXPECT_EQ(rate1.TryAcquire(rate_key, RatePriority::Cancel, 1, mono),
            ErrorCode::kRateBreakerOpen);

  OrderStrategyIndex route(
      [](const ClientOrderId& id) -> std::optional<uint64_t> {
        if (id.value.size() != 2 || id.value[0] != 'C' || id.value[1] < '1' ||
            id.value[1] > '8')
          return std::nullopt;
        return static_cast<uint64_t>(id.value[1] - '0');
      });
  ASSERT_TRUE(route.SetStrategyRoute(owner0, account, ShardId{0}).ok());
  ASSERT_TRUE(route.SetStrategyRoute(owner1, account, ShardId{1}).ok());
  ASSERT_TRUE(
      route.RegisterClient(ClientOrderId("C1"), account, market, owner0).ok());
  ASSERT_TRUE(
      route.RegisterClient(ClientOrderId("C2"), account, market, owner1).ok());
  auto router = AccountReportRouter::Create({ShardId{0}, 1, 2}, route);
  ASSERT_TRUE(router.ok()) << router.status();
  OrderUpdate update;
  update.account = account;
  update.market = market;
  update.client_order_id = ClientOrderId("C2");
  update.status = ExchangeOrderStatus::Open;
  auto routed = (*router)->Route(update);
  EXPECT_EQ(routed.disposition, RouteDisposition::Forwarded);
  ASSERT_TRUE(routed.wake_shard);
  EXPECT_EQ(routed.wake_shard->value, 1);
  auto overflow = (*router)->Route(update);
  EXPECT_EQ(overflow.failure, ErrorCode::kRouteQueueFull);
  EXPECT_TRUE(overflow.pause_account);
  EXPECT_TRUE(overflow.request_order_query);
  EXPECT_TRUE((*router)->IsAccountPaused(account));
  auto consumed = (*router)->TryPop(ShardId{1});
  ASSERT_TRUE(consumed);
  EXPECT_EQ(consumed->strategy_id.value, 2);
  (*router)->MarkOrderQueried(account);
  EXPECT_FALSE((*router)->IsAccountPaused(account));
  update.client_order_id = ClientOrderId("BAD");
  auto unknown = (*router)->Route(update);
  EXPECT_EQ(unknown.disposition, RouteDisposition::Quarantined);
  EXPECT_TRUE(unknown.pause_account);
  EXPECT_TRUE(unknown.request_order_query);
}

TEST(MultiShardTest, EightShardRouterQueuesStaySeparate) {
  const AccountId account("A");
  const MarketId market{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
  OrderStrategyIndex route(
      [](const ClientOrderId& id) -> std::optional<uint64_t> {
        if (id.value.size() != 2 || id.value[0] != 'C' || id.value[1] < '1' ||
            id.value[1] > '8')
          return std::nullopt;
        return static_cast<uint64_t>(id.value[1] - '0');
      });
  for (uint8_t shard = 0; shard < 8; ++shard) {
    const uint64_t key = shard + 1;
    const StrategyId strategy_id{key, StrategyName("pmm")};
    const ClientOrderId client("C" + std::to_string(key));
    ASSERT_TRUE(
        route.SetStrategyRoute(strategy_id, account, ShardId{shard}).ok());
    ASSERT_TRUE(
        route.RegisterClient(client, account, market, strategy_id).ok());
  }
  auto router = AccountReportRouter::Create({ShardId{0}, 2, 2}, route);
  ASSERT_TRUE(router.ok()) << router.status();
  for (uint8_t shard = 0; shard < 8; ++shard) {
    OrderUpdate update;
    update.account = account;
    update.market = market;
    update.client_order_id = ClientOrderId("C" + std::to_string(shard + 1));
    const auto result = (*router)->Route(update);
    EXPECT_EQ(result.failure, ErrorCode::kOk);
    if (shard == 0) {
      EXPECT_EQ(result.disposition, RouteDisposition::Local);
    } else {
      EXPECT_EQ(result.disposition, RouteDisposition::Forwarded);
      auto popped = (*router)->TryPop(ShardId{shard});
      ASSERT_TRUE(popped);
      EXPECT_EQ(popped->strategy_id.value, shard + 1);
    }
  }
}

}  // namespace
}  // namespace hquant
