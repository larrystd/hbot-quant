#include <algorithm>
#include <chrono>
#include <optional>
#include <utility>
#include <variant>
#include <vector>

#include "gtest/gtest.h"
#include "order/risk.h"
#include "order/simulated_exchange.h"
#include "shard/shard.h"
#include "strategy/strategy.h"

namespace hquant {
namespace {

Decimal D(const char* text) { return *Decimal::Parse(text); }

class MemoryWriter final : public OrderHistoryWriter {
 public:
  bool TryPush(OrderHistoryRecord record) override {
    rows.push_back(std::move(record));
    return true;
  }
  std::vector<OrderHistoryRecord> rows;
};

class ImmediateFillExchange final : public SimulatedExchange {
 public:
  ImmediateFillExchange(SimulatedExchangeConfig config, const Clock& clock)
      : inner_(std::move(config), clock) {}
  absl::StatusOr<PreparedOrder> PrepareSubmit(ApprovedOrder order) override {
    return inner_.PrepareSubmit(std::move(order));
  }
  absl::Status StartPrepared(const ClientOrderId& id) override {
    auto status = inner_.StartPrepared(id);
    if (!status.ok()) return status;
    return inner_.OnPublicTrade(Side::Buy, D("101"), D("0.01"));
  }
  absl::Status AbortPrepared(const ClientOrderId& id) override {
    return inner_.AbortPrepared(id);
  }
  absl::Status StartCancel(const StrategyId& strategy_id,
                           const ClientOrderId& id) override {
    return inner_.StartCancel(strategy_id, id);
  }
  absl::Status OnBookBbo(const Decimal& bid, const Decimal& ask) override {
    return inner_.OnBookBbo(bid, ask);
  }
  absl::Status OnPublicTrade(Side side, const Decimal& price,
                             const Decimal& amount) override {
    return inner_.OnPublicTrade(side, price, amount);
  }
  Decimal BalanceOf(const AssetId& asset) const override {
    return inner_.BalanceOf(asset);
  }
  Decimal AvailableBalance(const AssetId& asset) const override {
    return inner_.AvailableBalance(asset);
  }
  std::vector<AccountEvent> DrainEvents() override {
    return inner_.DrainEvents();
  }

 private:
  SimpleSimulatedExchange inner_;
};

class CountingStrategy final : public Strategy {
 public:
  TriggerPolicy policy;
  bool submit_once = false;
  bool deciding = false;
  bool recursed = false;
  std::vector<Trigger> calls;
  MarketId market;
  AccountId account;
  StrategyId id;

  TriggerPolicy Triggers() const override { return policy; }
  ActionBatch Decide(const StrategyInput& input, Trigger why) override {
    if (deciding) recursed = true;
    deciding = true;
    calls.push_back(why);
    ActionBatch actions;
    if (submit_once && input.ready) {
      submit_once = false;
      OrderRequest order;
      order.account = account;
      order.market = market;
      order.side = Side::Sell;
      order.type = OrderType::Limit;
      order.quantity = D("0.01");
      order.limit_price = D("100.1");
      order.time_in_force = TimeInForce::Gtc;
      actions.ordered.emplace_back(SubmitOrder{id, order});
    }
    deciding = false;
    return actions;
  }
};

class TriggerFixture : public ::testing::Test {
 protected:
  UtcTime origin{std::chrono::microseconds(1'000'000)};
  ReplayClock clock{origin, MonoTime(std::chrono::microseconds(1'000'000))};
  AccountId account{"simulated"};
  MarketId market{ExchangeId("simulated"), InstrumentKind::Spot, "BTCUSDT"};
  MarketSpec spec{market, AssetId("BTC"), AssetId("USDT")};
  StrategyId id{1, StrategyName("counter")};
  TickLotSize scale{D("0.01"), D("0.001"), 1};
  TradingRule rule{market,    D("0.01"), D("0.001"), D("0.001"),
                   D("0.01"), {},        1,          origin};
  CountingStrategy strategy;
  SimpleSimulatedExchange exchange{{account,
                                    spec,
                                    rule,
                                    {{"BTC", D("1")}, {"USDT", D("100")}},
                                    D("0.001"),
                                    true,
                                    {}},
                                   clock};
  RiskGate risk{{ShardId{0}, D("0"), std::chrono::seconds(300)}};
  MemoryWriter writer;
  Shard shard{{RunId{1}, ShardId{0}, id, account, spec, scale, rule},
              clock,
              strategy,
              exchange,
              risk,
              writer};
  int64_t current_bid = 9999;

  void SetUp() override {
    strategy.policy.book_mode = BookTriggerMode::BboChanged;
    strategy.market = market;
    strategy.account = account;
    strategy.id = id;
    ASSERT_TRUE(risk.SetInitialBudget({account, AssetId("BTC"), ShardId{0}, 1,
                                       D("1"), origin + std::chrono::hours(1)})
                    .ok());
    ASSERT_TRUE(
        risk.SetInitialBudget({account, AssetId("USDT"), ShardId{0}, 1,
                               D("100"), origin + std::chrono::hours(1)})
            .ok());
    ASSERT_TRUE(clock.Advance({0, 1}).ok());
    shard.Subscribe(1);
    BookSnapshot snapshot{market,
                          1,
                          1,
                          10,
                          {{{9999}, {100}}},
                          {{{10001}, {100}}},
                          {{}, clock.UtcNow(), clock.MonoNow()}};
    ASSERT_TRUE(shard.OnSnapshot(snapshot).ok());
    BookDiff bridge{market, 1,  1,  11,
                    11,     {}, {}, {{}, clock.UtcNow(), clock.MonoNow()}};
    ASSERT_TRUE(shard.OnDiff(bridge).ok());
    ASSERT_EQ(shard.Book().State(), BookSyncState::Live);
  }

  void ChangeBid(int64_t at_us, uint64_t sequence, int64_t new_bid) {
    ASSERT_TRUE(clock.Advance({at_us, sequence}).ok());
    BookDiff diff{market,   1,
                  1,        sequence,
                  sequence, {{{current_bid}, {0}}, {{new_bid}, {100}}},
                  {},       {{}, clock.UtcNow(), clock.MonoNow()}};
    ASSERT_TRUE(shard.OnDiff(diff).ok());
    current_bid = new_bid;
  }
};

TEST_F(TriggerFixture, BboChangesAndPublicTradesTriggerDeterministically) {
  ASSERT_EQ(strategy.calls, std::vector<Trigger>{Trigger::BookChanged});
  ASSERT_TRUE(clock.Advance({1, 1}).ok());
  BookDiff unchanged{market, 1,  1,  12,
                     12,     {}, {}, {{}, clock.UtcNow(), clock.MonoNow()}};
  ASSERT_TRUE(shard.OnDiff(unchanged).ok());
  EXPECT_EQ(strategy.calls.size(), 1);
  ChangeBid(2, 13, 9998);
  ASSERT_EQ(strategy.calls.size(), 2);
  EXPECT_EQ(strategy.calls.back(), Trigger::BookChanged);
  strategy.policy.on_public_trade = true;
  ASSERT_TRUE(clock.Advance({3, 1}).ok());
  PublicTrade trade{market, {},        {10100},
                    {10},   Side::Buy, {{}, clock.UtcNow(), clock.MonoNow()}};
  ASSERT_TRUE(shard.OnPublicTrade(trade).ok());
  ASSERT_EQ(strategy.calls.size(), 3);
  EXPECT_EQ(strategy.calls.back(), Trigger::PublicTraded);
}

TEST_F(TriggerFixture, CoalescesAndLimitsStrategyCallsUsingReplayClock) {
  strategy.policy.coalesce_window = std::chrono::microseconds(10);
  strategy.policy.min_action_interval = std::chrono::microseconds(5);
  ASSERT_TRUE(clock.Advance({1, 1}).ok());
  BookDiff unchanged{market, 1,  1,  12,
                     12,     {}, {}, {{}, clock.UtcNow(), clock.MonoNow()}};
  ASSERT_TRUE(shard.OnDiff(unchanged).ok());
  ChangeBid(2, 13, 9998);
  EXPECT_EQ(strategy.calls.size(), 1);
  ChangeBid(11, 14, 9997);
  EXPECT_EQ(strategy.calls.size(), 2);
  strategy.policy.coalesce_window.reset();
  ChangeBid(15, 15, 9996);
  EXPECT_EQ(strategy.calls.size(), 2);
  ChangeBid(17, 16, 9995);
  EXPECT_EQ(strategy.calls.size(), 3);
}

TEST_F(TriggerFixture, OwnOrderAndFillTriggersDoNotReenterDecide) {
  strategy.policy.on_order_update = true;
  strategy.policy.on_fill = true;
  strategy.submit_once = true;
  ChangeBid(1, 12, 9998);
  ASSERT_EQ(strategy.calls.size(), 3);  // Book, changed book, open report.
  EXPECT_EQ(strategy.calls.back(), Trigger::OrderUpdated);
  EXPECT_FALSE(strategy.recursed);
  ASSERT_EQ(exchange.OpenOrders().size(), 1);
  ASSERT_TRUE(clock.Advance({2, 1}).ok());
  PublicTrade trade{market, {},        {10100},
                    {10},   Side::Buy, {{}, clock.UtcNow(), clock.MonoNow()}};
  ASSERT_TRUE(shard.OnPublicTrade(trade).ok());
  ASSERT_EQ(strategy.calls.size(), 4);
  EXPECT_EQ(strategy.calls.back(), Trigger::Traded);
  EXPECT_FALSE(strategy.recursed);
  EXPECT_TRUE(exchange.OpenOrders().empty());
}

TEST(ShardTriggerTest, ImmediateFillQueuesOneFollowupWithoutRecursion) {
  const UtcTime origin(std::chrono::microseconds(1'000'000));
  ReplayClock clock(origin, MonoTime(std::chrono::microseconds(1'000'000)));
  const AccountId account("simulated");
  const MarketId market{ExchangeId("simulated"), InstrumentKind::Spot,
                        "BTCUSDT"};
  const MarketSpec spec{market, AssetId("BTC"), AssetId("USDT")};
  const StrategyId id{1, StrategyName("counter")};
  const TickLotSize scale{D("0.01"), D("0.001"), 1};
  const TradingRule rule{market,    D("0.01"), D("0.001"), D("0.001"),
                         D("0.01"), {},        1,          origin};
  CountingStrategy strategy;
  strategy.policy.book_mode = BookTriggerMode::BboChanged;
  strategy.policy.on_fill = true;
  strategy.policy.on_order_update = true;
  strategy.submit_once = true;
  strategy.market = market;
  strategy.account = account;
  strategy.id = id;
  ImmediateFillExchange exchange({account,
                                  spec,
                                  rule,
                                  {{"BTC", D("1")}, {"USDT", D("100")}},
                                  D("0.001"),
                                  true,
                                  {}},
                                 clock);
  RiskGate risk({ShardId{0}, D("0"), std::chrono::seconds(300)});
  ASSERT_TRUE(risk.SetInitialBudget({account, AssetId("BTC"), ShardId{0}, 1,
                                     D("1"), origin + std::chrono::hours(1)})
                  .ok());
  ASSERT_TRUE(risk.SetInitialBudget({account, AssetId("USDT"), ShardId{0}, 1,
                                     D("100"), origin + std::chrono::hours(1)})
                  .ok());
  MemoryWriter writer;
  Shard shard({RunId{1}, ShardId{0}, id, account, spec, scale, rule}, clock,
              strategy, exchange, risk, writer);
  ASSERT_TRUE(clock.Advance({0, 1}).ok());
  shard.Subscribe(1);
  BookSnapshot snapshot{market,
                        1,
                        1,
                        10,
                        {{{9999}, {100}}},
                        {{{10001}, {100}}},
                        {{}, clock.UtcNow(), clock.MonoNow()}};
  ASSERT_TRUE(shard.OnSnapshot(snapshot).ok());
  BookDiff bridge{market, 1,  1,  11,
                  11,     {}, {}, {{}, clock.UtcNow(), clock.MonoNow()}};
  ASSERT_TRUE(shard.OnDiff(bridge).ok());
  ASSERT_EQ(strategy.calls.size(), 2);
  EXPECT_EQ(strategy.calls[0], Trigger::BookChanged);
  EXPECT_EQ(strategy.calls[1], Trigger::Traded);
  EXPECT_FALSE(strategy.recursed);
  EXPECT_TRUE(std::any_of(writer.rows.begin(), writer.rows.end(),
                          [](const OrderHistoryRecord& row) {
                            return std::holds_alternative<TradeUpdate>(
                                row.payload);
                          }));
}

class ReplayHarness final : public TriggerFixture {
 public:
  void TestBody() override {}
  std::vector<Trigger> Replay() {
    SetUp();
    ChangeBid(1, 12, 9998);
    strategy.policy.on_public_trade = true;
    if (!clock.Advance({2, 1}).ok()) return {};
    PublicTrade trade{market, {},        {10100},
                      {10},   Side::Buy, {{}, clock.UtcNow(), clock.MonoNow()}};
    if (!shard.OnPublicTrade(trade).ok()) return {};
    return strategy.calls;
  }
};

TEST(ShardTriggerDeterminismTest, SameInputsProduceSameCalls) {
  ReplayHarness first;
  ReplayHarness second;
  EXPECT_EQ(first.Replay(), second.Replay());
}

}  // namespace
}  // namespace hquant
