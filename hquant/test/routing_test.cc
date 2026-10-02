#include "shard/routing.h"

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include "gtest/gtest.h"

namespace hquant {
namespace {

MarketId Market(std::string symbol = "BTCUSDT") {
  return MarketId{ExchangeId("binance"), InstrumentKind::Spot,
                  std::move(symbol)};
}

StrategyId MakeStrategyId(uint64_t key) {
  return StrategyId{key, StrategyName("simple_pmm")};
}

OrderUpdate Update(std::string client, std::string exchange = "") {
  OrderUpdate update;
  update.account = AccountId("account-a");
  update.market = Market();
  if (!client.empty()) update.client_order_id = ClientOrderId(std::move(client));
  if (!exchange.empty())
    update.exchange_order_id = ExchangeOrderId(std::move(exchange));
  return update;
}

TradeUpdate Trade(std::string client, std::string exchange = "") {
  TradeUpdate trade;
  trade.account = AccountId("account-a");
  trade.market = Market();
  if (!client.empty()) trade.client_order_id = ClientOrderId(std::move(client));
  if (!exchange.empty())
    trade.exchange_order_id = ExchangeOrderId(std::move(exchange));
  trade.exchange_trade_id = ExchangeTradeId("T1");
  return trade;
}

OrderStrategyIndex Index() {
  // Test codec validates the whole synthetic ID. The digit represents only
  // the stable strategy_id; a trailing shard hint has no routing authority.
  return OrderStrategyIndex(
      [](const ClientOrderId& id) -> std::optional<uint64_t> {
        if (id.value.size() != 4 || id.value[0] != 'C' || id.value[1] < '1' ||
            id.value[1] > '9' || id.value[2] != '-' || id.value[3] < '0' ||
            id.value[3] > '7') {
          return std::nullopt;
        }
        return static_cast<uint64_t>(id.value[1] - '0');
      });
}

TEST(OrderStrategyIndexTest, StableStrategyRoutesOldClientToCurrentShard) {
  auto index = Index();
  ASSERT_TRUE(index
                  .SetStrategyRoute(MakeStrategyId(1), AccountId("account-a"),
                                    ShardId{1})
                  .ok());
  ASSERT_TRUE(index
                  .RegisterClient(ClientOrderId("C1-1"), AccountId("account-a"),
                                  Market(), MakeStrategyId(1))
                  .ok());
  ASSERT_TRUE(index
                  .RegisterExchange(AccountId("account-a"), Market(),
                                    ExchangeOrderId("E1"), MakeStrategyId(1),
                                    ClientOrderId("C1-1"))
                  .ok());
  ASSERT_TRUE(index
                  .SetStrategyRoute(MakeStrategyId(1), AccountId("account-a"),
                                    ShardId{5})
                  .ok());
  auto from_old_client = index.Resolve(AccountId("account-a"), Market(),
                                       ClientOrderId("C1-1"), std::nullopt);
  ASSERT_TRUE(from_old_client.ok()) << from_old_client.status();
  EXPECT_EQ(from_old_client->current_shard.value, 5);
  auto from_exchange = index.Resolve(AccountId("account-a"), Market(),
                                     std::nullopt, ExchangeOrderId("E1"));
  ASSERT_TRUE(from_exchange.ok()) << from_exchange.status();
  EXPECT_EQ(from_exchange->strategy_id, MakeStrategyId(1));
  auto decoded_only = index.Resolve(AccountId("account-a"), Market(),
                                    ClientOrderId("C1-7"), std::nullopt);
  ASSERT_TRUE(decoded_only.ok());
  EXPECT_EQ(decoded_only->current_shard.value, 5);
}

TEST(OrderStrategyIndexTest, RejectsConflictsAndWrongAccountOrMarket) {
  auto index = Index();
  ASSERT_TRUE(index
                  .SetStrategyRoute(MakeStrategyId(1), AccountId("account-a"),
                                    ShardId{1})
                  .ok());
  ASSERT_TRUE(index
                  .SetStrategyRoute(MakeStrategyId(2), AccountId("account-a"),
                                    ShardId{2})
                  .ok());
  EXPECT_FALSE(index
                   .SetStrategyRoute(MakeStrategyId(1), AccountId("account-b"),
                                     ShardId{3})
                   .ok());
  EXPECT_FALSE(index
                   .RegisterClient(ClientOrderId("C2-0"),
                                   AccountId("account-a"), Market(),
                                   MakeStrategyId(1))
                   .ok());
  ASSERT_TRUE(index
                  .RegisterClient(ClientOrderId("C1-0"), AccountId("account-a"),
                                  Market(), MakeStrategyId(1))
                  .ok());
  EXPECT_FALSE(index
                   .RegisterClient(ClientOrderId("C1-0"),
                                   AccountId("account-a"), Market("ETHUSDT"),
                                   MakeStrategyId(1))
                   .ok());
  ASSERT_TRUE(index
                  .RegisterExchange(AccountId("account-a"), Market(),
                                    ExchangeOrderId("E2"), MakeStrategyId(2))
                  .ok());
  EXPECT_FALSE(index
                   .RegisterExchange(AccountId("account-a"), Market(),
                                     ExchangeOrderId("E2"), MakeStrategyId(1))
                   .ok());
  EXPECT_FALSE(index
                   .Resolve(AccountId("account-a"), Market(),
                            ClientOrderId("C1-0"), ExchangeOrderId("E2"))
                   .ok());
  EXPECT_FALSE(index
                   .Resolve(AccountId("account-b"), Market(),
                            ClientOrderId("C1-0"), std::nullopt)
                   .ok());
  EXPECT_FALSE(index
                   .Resolve(AccountId("account-a"), Market("ETHUSDT"),
                            ClientOrderId("C1-0"), std::nullopt)
                   .ok());
  EXPECT_FALSE(
      index
          .Resolve(AccountId("account-a"), Market(), std::nullopt, std::nullopt)
          .ok());
}

TEST(AccountReportRouterTest,
     LocalAndRemoteDeliveryPreserveStrategyAndSourceOrder) {
  auto index = Index();
  ASSERT_TRUE(index
                  .SetStrategyRoute(MakeStrategyId(1), AccountId("account-a"),
                                    ShardId{0})
                  .ok());
  ASSERT_TRUE(index
                  .SetStrategyRoute(MakeStrategyId(2), AccountId("account-a"),
                                    ShardId{2})
                  .ok());
  auto router_or = AccountReportRouter::Create({ShardId{0}, 2, 2}, index);
  ASSERT_TRUE(router_or.ok());
  auto& router = **router_or;

  auto local = router.Route(Update("C1-7"));
  ASSERT_EQ(local.disposition, RouteDisposition::Local);
  ASSERT_TRUE(local.local_report);
  EXPECT_EQ(local.local_report->strategy_id, MakeStrategyId(1));
  EXPECT_EQ(local.source_sequence, 1);
  auto first = router.Route(Trade("C2-0"));
  EXPECT_EQ(first.disposition, RouteDisposition::Forwarded);
  ASSERT_TRUE(first.wake_shard);
  EXPECT_EQ(first.wake_shard->value, 2);
  auto second = router.Route(Update("C2-7"));
  EXPECT_EQ(second.disposition, RouteDisposition::Forwarded);
  EXPECT_FALSE(second.wake_shard);
  auto popped_first = router.TryPop(ShardId{2});
  auto popped_second = router.TryPop(ShardId{2});
  ASSERT_TRUE(popped_first && popped_second);
  EXPECT_EQ(popped_first->source_sequence, 2);
  EXPECT_EQ(popped_second->source_sequence, 3);
  EXPECT_TRUE(std::holds_alternative<TradeUpdate>(popped_first->report));
  EXPECT_TRUE(std::holds_alternative<OrderUpdate>(popped_second->report));
  EXPECT_FALSE(router.TryPop(ShardId{2}));
}

TEST(AccountReportRouterTest,
     QueueFullAndUnknownIsolatePauseAndRequireReconcile) {
  auto index = Index();
  ASSERT_TRUE(index
                  .SetStrategyRoute(MakeStrategyId(2), AccountId("account-a"),
                                    ShardId{2})
                  .ok());
  auto router_or = AccountReportRouter::Create({ShardId{0}, 1, 2}, index);
  ASSERT_TRUE(router_or.ok());
  auto& router = **router_or;

  EXPECT_EQ(router.Route(Update("C2-0")).disposition,
            RouteDisposition::Forwarded);
  auto full = router.Route(Update("C2-1"));
  EXPECT_EQ(full.disposition, RouteDisposition::Quarantined);
  EXPECT_EQ(full.failure, ErrorCode::kRouteQueueFull);
  EXPECT_TRUE(full.pause_account && full.request_order_query);
  EXPECT_TRUE(router.IsAccountPaused(AccountId("account-a")));
  auto unknown = router.Route(Trade("unrecognized"));
  EXPECT_EQ(unknown.failure, ErrorCode::kRouteStrategyUnknown);
  EXPECT_EQ(router.QuarantineSize(), 2);
  auto dropped = router.Route(Update(""));
  EXPECT_TRUE(dropped.quarantine_dropped);
  EXPECT_EQ(router.DroppedQuarantineCount(), 1);
  EXPECT_EQ(router.TryPopQuarantined()->source_sequence, 2);
  EXPECT_EQ(router.TryPopQuarantined()->source_sequence, 3);
  EXPECT_FALSE(router.TryPopQuarantined());
  router.MarkOrderQueried(AccountId("account-a"));
  EXPECT_FALSE(router.IsAccountPaused(AccountId("account-a")));
}

TEST(AccountReportRouterTest, RemoteQueueIsSafeForOneReaderAndOneShard) {
  auto index = Index();
  ASSERT_TRUE(index
                  .SetStrategyRoute(MakeStrategyId(2), AccountId("account-a"),
                                    ShardId{2})
                  .ok());
  auto router_or = AccountReportRouter::Create({ShardId{0}, 64, 8}, index);
  ASSERT_TRUE(router_or.ok());
  auto& router = **router_or;
  constexpr uint64_t kCount = 10'000;
  std::atomic<uint64_t> received{0};
  std::atomic<bool> stop{false};
  std::atomic<bool> bad_order{false};
  bool queue_full = false;
  std::thread consumer([&] {
    uint64_t expected = 1;
    while (expected <= kCount && !stop.load(std::memory_order_relaxed)) {
      auto report = router.TryPop(ShardId{2});
      if (!report) {
        std::this_thread::yield();
        continue;
      }
      if (report->source_sequence != expected ||
          report->strategy_id != MakeStrategyId(2)) {
        bad_order.store(true, std::memory_order_relaxed);
        return;
      }
      received.store(expected++, std::memory_order_relaxed);
    }
  });
  for (uint64_t i = 0; i < kCount; ++i) {
    auto result = router.Route(Update("C2-0"));
    if (result.failure == ErrorCode::kRouteQueueFull) {
      queue_full = true;
      break;
    }
    while (received.load(std::memory_order_relaxed) + 32 < i + 1 &&
           !bad_order.load(std::memory_order_relaxed)) {
      std::this_thread::yield();
    }
    if (bad_order.load(std::memory_order_relaxed)) break;
  }
  if (queue_full) stop.store(true, std::memory_order_relaxed);
  consumer.join();
  EXPECT_FALSE(queue_full);
  EXPECT_FALSE(bad_order);
  EXPECT_EQ(received.load(), kCount);
}

}  // namespace
}  // namespace hquant
