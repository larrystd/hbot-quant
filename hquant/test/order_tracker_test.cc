#include "order/order_tracker.h"

#include "base/error.h"
#include "gtest/gtest.h"

namespace hquant {
namespace {

Decimal D(const char* text) { return *Decimal::Parse(text); }

Order MakeOrder(const char* id = "B1") {
  return Order{
      ClientOrderId(id),
      StrategyId{1, StrategyName("simple_pmm")},
      AccountId("A1"),
      MarketId{ExchangeId("simulated"), InstrumentKind::Spot, "BTC-USDT"},
      Side::Buy,
      D("1"),
      D("100"),
      TimeInForce::Gtc};
}

OrderUpdate Update(const Order& order, ExchangeOrderStatus status) {
  OrderUpdate update;
  update.account = order.account;
  update.market = order.market;
  update.client_order_id = order.client_order_id;
  update.status = status;
  return update;
}

Trade Fill(const char* id, const char* quantity) {
  Trade trade;
  trade.exchange_trade_id = ExchangeTradeId(id);
  trade.price = D("100");
  trade.quantity = D(quantity);
  trade.value = *trade.price.Multiply(trade.quantity);
  return trade;
}

TEST(OrderTrackerTest, AddAndSendOutcomes) {
  OrderTracker tracker;
  const auto order = MakeOrder();
  ASSERT_TRUE(tracker.Add(order).ok());
  EXPECT_EQ(*tracker.Find(order.client_order_id)->price.Compare(D("100")), 0);
  EXPECT_EQ(tracker.ActiveOrders().size(), 1);
  EXPECT_EQ(CodeOf(tracker.Add(order)), ErrorCode::kOrderDuplicate);
  auto unknown =
      tracker.OnSendResult(order.client_order_id, SendResult::Unknown);
  ASSERT_TRUE(unknown.ok()) << unknown.status();
  EXPECT_EQ(*unknown, UpdateResult::Updated);
  EXPECT_EQ(tracker.OrdersToQuery().size(), 1);
  auto ack = Update(order, ExchangeOrderStatus::Open);
  EXPECT_EQ(*tracker.Apply(ack, ReportSource::Push), UpdateResult::Updated);
  EXPECT_TRUE(tracker.OrdersToQuery().empty());

  const auto unsent = MakeOrder("B2");
  ASSERT_TRUE(tracker.Add(unsent).ok());
  EXPECT_EQ(*tracker.OnSendResult(unsent.client_order_id, SendResult::NotSent),
            UpdateResult::Finished);
  EXPECT_EQ(tracker.ActiveOrders().size(), 1);
}

TEST(OrderTrackerTest, ATradeAndItsStatusAreAppliedOnce) {
  OrderTracker tracker;
  const auto order = MakeOrder();
  ASSERT_TRUE(tracker.Add(order).ok());
  auto update = Update(order, ExchangeOrderStatus::Traded);
  update.executed_quantity = D("1");
  update.trade = Fill("T1", "1");
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Finished);
  EXPECT_TRUE(tracker.HasTrade(ExchangeTradeId("T1")));
  EXPECT_TRUE(tracker.ActiveOrders().empty());
  EXPECT_TRUE(tracker.OrdersToQuery().empty());
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Ignored);
}

TEST(OrderTrackerTest, TerminalStatusWaitsForMissingTrades) {
  OrderTracker tracker;
  const auto order = MakeOrder();
  ASSERT_TRUE(tracker.Add(order).ok());
  auto update = Update(order, ExchangeOrderStatus::Traded);
  update.executed_quantity = D("1");
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Updated);
  EXPECT_TRUE(tracker.ActiveOrders().empty());
  EXPECT_EQ(tracker.OrdersToQuery().size(), 1);
  update.trade = Fill("T1", "0.4");
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Updated);
  update.trade = Fill("T2", "0.6");
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Finished);
  EXPECT_TRUE(tracker.OrdersToQuery().empty());
}

TEST(OrderTrackerTest, DuplicateTradeCanCarryAChangedStatus) {
  OrderTracker tracker;
  const auto order = MakeOrder();
  ASSERT_TRUE(tracker.Add(order).ok());
  auto update = Update(order, ExchangeOrderStatus::PartiallyTraded);
  update.executed_quantity = D("0.4");
  update.trade = Fill("T1", "0.4");
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Updated);
  EXPECT_TRUE(tracker.HasTrade(update.trade->exchange_trade_id));
  update.status = ExchangeOrderStatus::Canceled;
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Finished);
  EXPECT_TRUE(tracker.ActiveOrders().empty());
}

TEST(OrderTrackerTest, QueryResolvesConflictingTerminalStatus) {
  OrderTracker tracker;
  const auto order = MakeOrder();
  ASSERT_TRUE(tracker.Add(order).ok());
  auto canceled = Update(order, ExchangeOrderStatus::Canceled);
  EXPECT_EQ(*tracker.Apply(canceled, ReportSource::Push),
            UpdateResult::Finished);
  auto expired = Update(order, ExchangeOrderStatus::Expired);
  EXPECT_EQ(CodeOf(tracker.Apply(expired, ReportSource::Push).status()),
            ErrorCode::kOrderStatusConflict);
  EXPECT_EQ(tracker.OrdersToQuery().size(), 1);
  // A later push cannot clear the conflict; only a verified query can.
  EXPECT_EQ(*tracker.Apply(canceled, ReportSource::Push),
            UpdateResult::Ignored);
  EXPECT_EQ(tracker.OrdersToQuery().size(), 1);
  EXPECT_EQ(*tracker.Apply(expired, ReportSource::Query),
            UpdateResult::Finished);
  EXPECT_TRUE(tracker.OrdersToQuery().empty());
}

TEST(OrderTrackerTest, ConflictingTerminalPushStillAcceptsVerifiedTrade) {
  OrderTracker tracker;
  const auto order = MakeOrder();
  ASSERT_TRUE(tracker.Add(order).ok());
  auto canceled = Update(order, ExchangeOrderStatus::Canceled);
  canceled.executed_quantity = D("0.4");
  canceled.trade = Fill("T1", "0.4");
  EXPECT_EQ(*tracker.Apply(canceled, ReportSource::Push),
            UpdateResult::Finished);

  auto contradictory = Update(order, ExchangeOrderStatus::Expired);
  contradictory.trade = Fill("T2", "0.2");
  EXPECT_EQ(*tracker.Apply(contradictory, ReportSource::Push),
            UpdateResult::Updated);
  EXPECT_TRUE(tracker.HasTrade(ExchangeTradeId("T2")));
  EXPECT_EQ(tracker.OrdersToQuery().size(), 1);
  EXPECT_EQ(*tracker.Apply(contradictory, ReportSource::Push),
            UpdateResult::Ignored);
  EXPECT_EQ(tracker.OrdersToQuery().size(), 1);
}

TEST(OrderTrackerTest, ContradictoryCumulativeStillAcceptsVerifiedTrade) {
  OrderTracker tracker;
  const auto order = MakeOrder();
  ASSERT_TRUE(tracker.Add(order).ok());
  auto update = Update(order, ExchangeOrderStatus::PartiallyTraded);
  update.executed_quantity = D("0.1");
  update.trade = Fill("T1", "0.2");
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Updated);
  EXPECT_TRUE(tracker.HasTrade(ExchangeTradeId("T1")));
  EXPECT_EQ(tracker.OrdersToQuery().size(), 1);
}

TEST(OrderTrackerTest, RejectsOverfillAndTradeIdCollision) {
  OrderTracker tracker;
  const auto first = MakeOrder();
  const auto second = MakeOrder("B2");
  ASSERT_TRUE(tracker.Add(first).ok());
  ASSERT_TRUE(tracker.Add(second).ok());
  auto update = Update(first, ExchangeOrderStatus::PartiallyTraded);
  update.trade = Fill("T1", "0.5");
  EXPECT_EQ(*tracker.Apply(update, ReportSource::Push), UpdateResult::Updated);
  auto collision = Update(second, ExchangeOrderStatus::PartiallyTraded);
  collision.trade = Fill("T1", "0.5");
  EXPECT_EQ(CodeOf(tracker.Apply(collision, ReportSource::Push).status()),
            ErrorCode::kTradeIdOnOtherOrder);
  update.trade = Fill("T2", "0.6");
  EXPECT_EQ(CodeOf(tracker.Apply(update, ReportSource::Push).status()),
            ErrorCode::kTradeExceedsOrderAmount);
  EXPECT_EQ(tracker.OrdersToQuery().size(), 2);
}

}  // namespace
}  // namespace hquant
