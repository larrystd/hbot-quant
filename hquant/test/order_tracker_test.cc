#include "order/order_tracker.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "base/error.h"
#include "gtest/gtest.h"
#include "hquant/test/fixture_loader.h"
#include "simdjson.h"

namespace hquant {
namespace {

std::string String(simdjson::dom::element value, const char* key) {
  std::string_view text;
  if (value[key].get(text))
    throw std::runtime_error(std::string("missing ") + key);
  return std::string(text);
}
std::optional<std::string> OptionalString(simdjson::dom::element value,
                                          const char* key) {
  simdjson::dom::element field;
  if (value[key].get(field) || field.is_null()) return std::nullopt;
  std::string_view text;
  if (field.get(text)) throw std::runtime_error(std::string("invalid ") + key);
  return std::string(text);
}
uint64_t Unsigned(simdjson::dom::element value, const char* key) {
  uint64_t number = 0;
  if (value[key].get(number))
    throw std::runtime_error(std::string("missing ") + key);
  return number;
}
Decimal D(std::string_view value) {
  auto parsed = Decimal::Parse(value);
  if (!parsed.ok())
    throw std::runtime_error(std::string(parsed.status().message()));
  return *parsed;
}
UtcTime At(int64_t us) { return UtcTime{std::chrono::microseconds{us}}; }
MonoTime MonoAt(int64_t us) { return MonoTime{std::chrono::microseconds{us}}; }
void ExpectDecimal(const Decimal& actual, std::string_view expected) {
  auto compared = actual.Compare(D(expected));
  ASSERT_TRUE(compared.ok()) << compared.status();
  EXPECT_EQ(*compared, 0);
}
std::string State(OrderDisplayState state) {
  switch (state) {
    case OrderDisplayState::PendingCreate:
      return "PendingCreate";
    case OrderDisplayState::Open:
      return "Open";
    case OrderDisplayState::PartiallyTraded:
      return "PartiallyTraded";
    case OrderDisplayState::PendingCancel:
      return "PendingCancel";
    case OrderDisplayState::SubmissionUnknown:
      return "SubmissionUnknown";
    case OrderDisplayState::AwaitingTrades:
      return "AwaitingTrades";
    case OrderDisplayState::Traded:
      return "Traded";
    case OrderDisplayState::Canceled:
      return "Canceled";
    case OrderDisplayState::Failed:
      return "Failed";
    case OrderDisplayState::Expired:
      return "Expired";
  }
  return "Absent";
}
std::string EventName(const TrackedOrderEvent& event) {
  if (std::holds_alternative<OrderOpened>(event)) return "OrderOpened";
  if (std::holds_alternative<OrderTraded>(event)) return "OrderTraded";
  if (std::holds_alternative<OrderFullyTraded>(event))
    return "OrderFullyTraded";
  if (std::holds_alternative<OrderCanceled>(event)) return "OrderCanceled";
  return "OrderFailed";
}
ExchangeOrderStatus Status(const std::string& text) {
  if (text == "Open") return ExchangeOrderStatus::Open;
  if (text == "PartiallyTraded") return ExchangeOrderStatus::PartiallyTraded;
  if (text == "Traded") return ExchangeOrderStatus::Traded;
  if (text == "Canceled") return ExchangeOrderStatus::Canceled;
  if (text == "Rejected") return ExchangeOrderStatus::Rejected;
  if (text == "Expired") return ExchangeOrderStatus::Expired;
  throw std::runtime_error("unknown exchange status");
}

PreparedOrder PreparedFromFixture(simdjson::dom::element setup) {
  PreparedOrder prepared;
  prepared.client_order_id = ClientOrderId{String(setup, "client_order_id")};
  prepared.strategy_id =
      StrategyId{Unsigned(setup, "strategy_id"), StrategyName{"simple_pmm"}};
  prepared.request.account = AccountId{String(setup, "account")};
  prepared.request.market = MarketId{
      ExchangeId{"simulated"}, InstrumentKind::Spot, String(setup, "market")};
  prepared.request.side =
      String(setup, "side") == "Buy" ? Side::Buy : Side::Sell;
  prepared.request.type = OrderType::Limit;
  prepared.request.quantity = D(String(setup, "quantity"));
  prepared.request.limit_price = D(String(setup, "limit_price"));
  return prepared;
}

absl::StatusOr<TrackerResult> ReplayStep(OrderTracker& tracker,
                                         const PreparedOrder& prepared,
                                         const fixtures::FixtureStep& step) {
  simdjson::dom::parser parser;
  simdjson::dom::element event;
  if (parser.parse(step.event_json).get(event))
    return absl::InvalidArgumentError("fixture event JSON invalid");
  const std::string kind = String(event, "kind");
  if (kind == "register") return tracker.Register(prepared);
  if (kind == "cancel_requested")
    return tracker.RequestCancel(prepared.client_order_id);
  if (kind == "submission_unknown")
    return tracker.MarkSubmissionUnknown(prepared.client_order_id);
  if (kind == "order_update" || kind == "reconcile") {
    OrderUpdate update;
    update.account = prepared.request.account;
    update.market = prepared.request.market;
    if (auto id = OptionalString(event, "client_order_id"))
      update.client_order_id = ClientOrderId{*id};
    if (auto id = OptionalString(event, "exchange_order_id"))
      update.exchange_order_id = ExchangeOrderId{*id};
    update.exchange_status = Status(String(event, "exchange_status"));
    if (auto amount = OptionalString(event, "traded_quantity"))
      update.traded_quantity = D(*amount);
    if (auto amount = OptionalString(event, "traded_value"))
      update.traded_value = D(*amount);
    update.time.receive_utc = At(step.stamp.at_us);
    update.time.receive_mono = MonoAt(step.stamp.at_us);
    return kind == "reconcile" ? tracker.ApplyQueriedOrder(update)
                               : tracker.ApplyOrderUpdate(update);
  }
  if (kind == "trade_update") {
    TradeUpdate trade;
    trade.account = prepared.request.account;
    trade.market = prepared.request.market;
    if (auto id = OptionalString(event, "client_order_id"))
      trade.client_order_id = ClientOrderId{*id};
    if (auto id = OptionalString(event, "exchange_order_id"))
      trade.exchange_order_id = ExchangeOrderId{*id};
    trade.exchange_trade_id = ExchangeTradeId{String(event, "trade_id")};
    trade.price = D(String(event, "price"));
    trade.quantity = D(String(event, "quantity"));
    trade.value = D(String(event, "value"));
    simdjson::dom::object fees;
    if (event["fees_by_asset"].get(fees))
      return absl::InvalidArgumentError("fixture fees missing");
    for (auto [asset, amount] : fees) {
      std::string_view text;
      if (amount.get(text))
        return absl::InvalidArgumentError("fixture fee invalid");
      trade.fees.push_back(TradeFee{AssetId{std::string(asset)}, D(text)});
    }
    trade.time.receive_utc = At(step.stamp.at_us);
    trade.time.receive_mono = MonoAt(step.stamp.at_us);
    return tracker.ApplyTradeUpdate(trade);
  }
  return absl::InvalidArgumentError("unknown fixture event");
}

void CheckOutput(const TrackerResult& result, const PreparedOrder& prepared,
                 const fixtures::FixtureStep& step) {
  simdjson::dom::parser parser;
  simdjson::dom::element expected;
  ASSERT_FALSE(parser.parse(step.output_json).get(expected));
  EXPECT_EQ(State(result.snapshot.display_state),
            String(expected, "display_state"));
  ExpectDecimal(result.snapshot.traded_quantity,
                String(expected, "traded_quantity"));
  ExpectDecimal(result.snapshot.traded_value, String(expected, "traded_value"));
  auto remaining =
      prepared.request.quantity.Subtract(result.snapshot.traded_quantity);
  ASSERT_TRUE(remaining.ok()) << remaining.status();
  ExpectDecimal(*remaining, String(expected, "remaining_base"));
  std::vector<std::string> events;
  for (const auto& event : result.events) events.push_back(EventName(event));
  simdjson::dom::array expected_events;
  ASSERT_FALSE(expected["events"].get(expected_events));
  std::vector<std::string> wanted;
  for (auto event : expected_events) {
    std::string_view name;
    ASSERT_FALSE(event.get(name));
    wanted.emplace_back(name);
  }
  EXPECT_EQ(events, wanted);
  std::map<std::string, Decimal> actual_fees;
  for (const auto& fee : result.snapshot.fees)
    actual_fees[fee.asset.value] = fee.signed_amount;
  simdjson::dom::object expected_fees;
  ASSERT_FALSE(expected["fees_by_asset"].get(expected_fees));
  size_t count = 0;
  for (auto [asset, amount] : expected_fees) {
    ++count;
    std::string_view text;
    ASSERT_FALSE(amount.get(text));
    auto found = actual_fees.find(std::string(asset));
    ASSERT_NE(found, actual_fees.end());
    ExpectDecimal(found->second, text);
  }
  EXPECT_EQ(actual_fees.size(), count);
}

TEST(OrderTrackerTest, ReplaysAllPinnedPythonAndArchitectureFixtures) {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  ASSERT_NE(srcdir, nullptr);
  ASSERT_NE(workspace, nullptr);
  const auto root = std::filesystem::path(srcdir) / workspace /
                    "hquant/test/fixtures/order_tracker";
  std::vector<std::filesystem::path> paths;
  for (const auto& entry : std::filesystem::directory_iterator(root))
    if (entry.path().extension() == ".json") paths.push_back(entry.path());
  std::sort(paths.begin(), paths.end());
  ASSERT_GE(paths.size(), 8);
  for (const auto& path : paths) {
    SCOPED_TRACE(path.string());
    auto fixture = fixtures::LoadFixture(path.string());
    ASSERT_TRUE(fixture.ok()) << fixture.status();
    simdjson::dom::parser setup_parser;
    simdjson::dom::element setup;
    ASSERT_FALSE(setup_parser.parse(fixture->setup_json).get(setup));
    const PreparedOrder prepared = PreparedFromFixture(setup);
    OrderTracker tracker;
    for (const auto& step : fixture->steps) {
      SCOPED_TRACE(step.stamp.at_us);
      auto result = ReplayStep(tracker, prepared, step);
      ASSERT_TRUE(result.ok()) << result.status();
      CheckOutput(*result, prepared, step);
    }
  }
}

TEST(OrderTrackerTest,
     RejectsIdCollisionAndOverfillWithoutMutatingVerifiedFills) {
  OrderTracker tracker;
  PreparedOrder first;
  first.client_order_id = ClientOrderId{"B1"};
  first.strategy_id = StrategyId{1, StrategyName{"s"}};
  first.request.account = AccountId{"A1"};
  first.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTC-USDT"};
  first.request.quantity = D("1");
  first.request.limit_price = D("100");
  ASSERT_TRUE(tracker.Register(first).ok());
  auto second = first;
  second.client_order_id = ClientOrderId{"B2"};
  ASSERT_TRUE(tracker.Register(second).ok());
  OrderUpdate ack;
  ack.account = first.request.account;
  ack.market = first.request.market;
  ack.client_order_id = first.client_order_id;
  ack.exchange_order_id = ExchangeOrderId{"E1"};
  ack.exchange_status = ExchangeOrderStatus::Open;
  ASSERT_TRUE(tracker.ApplyOrderUpdate(ack).ok());
  ack.client_order_id = second.client_order_id;
  EXPECT_EQ(CodeOf(tracker.ApplyOrderUpdate(ack).status()),
            ErrorCode::kExchangeOrderIdConflict);
  EXPECT_EQ(tracker.OrdersNeedingQuery().size(), 2);
  TradeUpdate trade;
  trade.account = first.request.account;
  trade.market = first.request.market;
  trade.client_order_id = first.client_order_id;
  trade.exchange_order_id = ExchangeOrderId{"E1"};
  trade.exchange_trade_id = ExchangeTradeId{"T1"};
  trade.price = D("100");
  trade.quantity = D("1.1");
  trade.value = D("110");
  EXPECT_EQ(CodeOf(tracker.ApplyTradeUpdate(trade).status()),
            ErrorCode::kTradeExceedsOrderAmount);
  auto snapshot = tracker.Snapshot(first.client_order_id);
  ASSERT_TRUE(snapshot);
  ExpectDecimal(snapshot->traded_quantity, "0");
}

TEST(OrderTrackerTest, ReconcilesUnknownSubmissionUsingOriginalClientId) {
  OrderTracker tracker;
  PreparedOrder prepared;
  prepared.client_order_id = ClientOrderId{"B1"};
  prepared.strategy_id = StrategyId{1, StrategyName{"s"}};
  prepared.request.account = AccountId{"A1"};
  prepared.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTC-USDT"};
  prepared.request.quantity = D("1");
  prepared.request.limit_price = D("100");
  ASSERT_TRUE(tracker.Register(prepared).ok());
  auto unknown = tracker.MarkSubmissionUnknown(prepared.client_order_id);
  ASSERT_TRUE(unknown.ok());
  EXPECT_EQ(unknown->snapshot.display_state,
            OrderDisplayState::SubmissionUnknown);
  ASSERT_EQ(tracker.OrdersNeedingQuery().size(), 1);
  OrderUpdate recovered;
  recovered.account = prepared.request.account;
  recovered.market = prepared.request.market;
  recovered.client_order_id = prepared.client_order_id;
  recovered.exchange_order_id = ExchangeOrderId{"E1"};
  recovered.exchange_status = ExchangeOrderStatus::Open;
  auto queried_order = tracker.ApplyQueriedOrder(recovered);
  ASSERT_TRUE(queried_order.ok()) << queried_order.status();
  EXPECT_EQ(queried_order->confirmation, ConfirmationState::Confirmed);
  EXPECT_EQ(queried_order->snapshot.display_state, OrderDisplayState::Open);
  ASSERT_EQ(queried_order->events.size(), 1);
  EXPECT_EQ(EventName(queried_order->events[0]), "OrderOpened");
  EXPECT_TRUE(tracker.OrdersNeedingQuery().empty());
  auto duplicate = tracker.ApplyOrderUpdate(recovered);
  ASSERT_TRUE(duplicate.ok());
  EXPECT_TRUE(duplicate->events.empty());
}

TEST(OrderTrackerTest, RoutesExchangeOnlyReportAndDoesNotRegressOnStaleStatus) {
  OrderTracker tracker;
  PreparedOrder prepared;
  prepared.client_order_id = ClientOrderId{"B1"};
  prepared.strategy_id = StrategyId{1, StrategyName{"s"}};
  prepared.request.account = AccountId{"A1"};
  prepared.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTC-USDT"};
  prepared.request.quantity = D("1");
  prepared.request.limit_price = D("100");
  ASSERT_TRUE(tracker.Register(prepared).ok());
  OrderUpdate update;
  update.account = prepared.request.account;
  update.market = prepared.request.market;
  update.client_order_id = prepared.client_order_id;
  update.exchange_order_id = ExchangeOrderId{"E1"};
  update.exchange_status = ExchangeOrderStatus::Open;
  ASSERT_TRUE(tracker.ApplyOrderUpdate(update).ok());
  update.client_order_id.reset();
  update.exchange_status = ExchangeOrderStatus::PartiallyTraded;
  auto partial = tracker.ApplyOrderUpdate(update);
  ASSERT_TRUE(partial.ok());
  EXPECT_EQ(partial->snapshot.display_state,
            OrderDisplayState::PartiallyTraded);
  update.exchange_status = ExchangeOrderStatus::Open;
  auto stale = tracker.ApplyOrderUpdate(update);
  ASSERT_TRUE(stale.ok());
  EXPECT_EQ(stale->snapshot.display_state, OrderDisplayState::PartiallyTraded);
  EXPECT_TRUE(stale->events.empty());
}

TEST(OrderTrackerTest,
     ProvenPreWriteFailureEndsPendingOrderWithoutCreatedEvent) {
  OrderTracker tracker;
  PreparedOrder prepared;
  prepared.client_order_id = ClientOrderId{"B1"};
  prepared.strategy_id = StrategyId{1, StrategyName{"s"}};
  prepared.request.account = AccountId{"A1"};
  prepared.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTC-USDT"};
  prepared.request.quantity = D("1");
  prepared.request.limit_price = D("100");
  ASSERT_TRUE(tracker.Register(prepared).ok());
  auto failed =
      tracker.FailBeforeWrite(prepared.client_order_id, "SendSlotUnavailable");
  ASSERT_TRUE(failed.ok()) << failed.status();
  EXPECT_EQ(failed->snapshot.display_state, OrderDisplayState::Failed);
  ASSERT_EQ(failed->events.size(), 1);
  EXPECT_EQ(EventName(failed->events[0]), "OrderFailed");
  EXPECT_EQ(std::get<OrderFailed>(failed->events[0]).reason,
            "SendSlotUnavailable");
  EXPECT_EQ(
      CodeOf(
          tracker.FailBeforeWrite(prepared.client_order_id, "again").status()),
      ErrorCode::kOrderNotCancelable);
}

}  // namespace
}  // namespace hquant
