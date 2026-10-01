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
  if (std::holds_alternative<OrderCreated>(event)) return "OrderCreated";
  if (std::holds_alternative<OrderFilled>(event)) return "OrderFilled";
  if (std::holds_alternative<OrderCompleted>(event)) return "OrderCompleted";
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

OrderIntent Intent(simdjson::dom::element setup) {
  OrderIntent intent;
  intent.client_id = ClientOrderId{String(setup, "client_id")};
  intent.strategy_id = StrategyId{Unsigned(setup, "strategy_id"), StrategyName{"simple_pmm"}};
  intent.request.account = AccountId{String(setup, "account")};
  intent.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, String(setup, "market")};
  intent.request.side = String(setup, "side") == "Buy" ? Side::Buy : Side::Sell;
  intent.request.type = OrderType::Limit;
  intent.request.base_amount = D(String(setup, "base_amount"));
  intent.request.limit_price = D(String(setup, "limit_price"));
  return intent;
}

absl::StatusOr<TrackerResult> ReplayStep(OrderTracker& tracker,
                                         const OrderIntent& intent,
                                         const fixtures::FixtureStep& step) {
  simdjson::dom::parser parser;
  simdjson::dom::element event;
  if (parser.parse(step.event_json).get(event))
    return absl::InvalidArgumentError("fixture event JSON invalid");
  const std::string kind = String(event, "kind");
  if (kind == "register") return tracker.Register(intent);
  if (kind == "cancel_requested")
    return tracker.RequestCancel(intent.client_id);
  if (kind == "submission_unknown")
    return tracker.MarkSubmissionUnknown(intent.client_id);
  if (kind == "order_update" || kind == "reconcile") {
    OrderUpdate update;
    update.account = intent.request.account;
    update.market = intent.request.market;
    if (auto id = OptionalString(event, "client_id"))
      update.client_id = ClientOrderId{*id};
    if (auto id = OptionalString(event, "exchange_order_id"))
      update.exchange_order_id = ExchangeOrderId{*id};
    update.exchange_status = Status(String(event, "exchange_status"));
    if (auto amount = OptionalString(event, "cumulative_base"))
      update.cumulative_base = D(*amount);
    if (auto amount = OptionalString(event, "cumulative_quote"))
      update.cumulative_quote = D(*amount);
    update.time.receive_utc = At(step.stamp.at_us);
    update.time.receive_mono = MonoAt(step.stamp.at_us);
    return kind == "reconcile" ? tracker.Reconcile(update)
                               : tracker.ApplyOrderUpdate(update);
  }
  if (kind == "trade_update") {
    TradeUpdate trade;
    trade.account = intent.request.account;
    trade.market = intent.request.market;
    if (auto id = OptionalString(event, "client_id"))
      trade.client_id = ClientOrderId{*id};
    if (auto id = OptionalString(event, "exchange_order_id"))
      trade.exchange_order_id = ExchangeOrderId{*id};
    trade.exchange_trade_id = ExchangeTradeId{String(event, "trade_id")};
    trade.price = D(String(event, "price"));
    trade.base_amount = D(String(event, "base_amount"));
    trade.quote_amount = D(String(event, "quote_amount"));
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

void CheckOutput(const TrackerResult& result, const OrderIntent& intent,
                 const fixtures::FixtureStep& step) {
  simdjson::dom::parser parser;
  simdjson::dom::element expected;
  ASSERT_FALSE(parser.parse(step.output_json).get(expected));
  EXPECT_EQ(State(result.snapshot.display_state),
            String(expected, "display_state"));
  ExpectDecimal(result.snapshot.cumulative_base,
                String(expected, "cumulative_base"));
  ExpectDecimal(result.snapshot.cumulative_quote,
                String(expected, "cumulative_quote"));
  auto remaining =
      intent.request.base_amount.Subtract(result.snapshot.cumulative_base);
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
    const OrderIntent intent = Intent(setup);
    OrderTracker tracker;
    for (const auto& step : fixture->steps) {
      SCOPED_TRACE(step.stamp.at_us);
      auto result = ReplayStep(tracker, intent, step);
      ASSERT_TRUE(result.ok()) << result.status();
      CheckOutput(*result, intent, step);
    }
  }
}

TEST(OrderTrackerTest,
     RejectsIdCollisionAndOverfillWithoutMutatingVerifiedFills) {
  OrderTracker tracker;
  OrderIntent first;
  first.client_id = ClientOrderId{"B1"};
  first.strategy_id = StrategyId{1, StrategyName{"s"}};
  first.request.account = AccountId{"A1"};
  first.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTC-USDT"};
  first.request.base_amount = D("1");
  first.request.limit_price = D("100");
  ASSERT_TRUE(tracker.Register(first).ok());
  auto second = first;
  second.client_id = ClientOrderId{"B2"};
  ASSERT_TRUE(tracker.Register(second).ok());
  OrderUpdate ack;
  ack.account = first.request.account;
  ack.market = first.request.market;
  ack.client_id = first.client_id;
  ack.exchange_order_id = ExchangeOrderId{"E1"};
  ack.exchange_status = ExchangeOrderStatus::Open;
  ASSERT_TRUE(tracker.ApplyOrderUpdate(ack).ok());
  ack.client_id = second.client_id;
  EXPECT_EQ(CodeOf(tracker.ApplyOrderUpdate(ack).status()),
            ErrorCode::kOrderReportConflict);
  EXPECT_EQ(tracker.ReconciliationQueue().size(), 2);
  TradeUpdate trade;
  trade.account = first.request.account;
  trade.market = first.request.market;
  trade.client_id = first.client_id;
  trade.exchange_order_id = ExchangeOrderId{"E1"};
  trade.exchange_trade_id = ExchangeTradeId{"T1"};
  trade.price = D("100");
  trade.base_amount = D("1.1");
  trade.quote_amount = D("110");
  EXPECT_EQ(CodeOf(tracker.ApplyTradeUpdate(trade).status()),
            ErrorCode::kOrderReportConflict);
  auto snapshot = tracker.Snapshot(first.client_id);
  ASSERT_TRUE(snapshot);
  ExpectDecimal(snapshot->cumulative_base, "0");
}

TEST(OrderTrackerTest, ReconcilesUnknownSubmissionUsingOriginalClientId) {
  OrderTracker tracker;
  OrderIntent intent;
  intent.client_id = ClientOrderId{"B1"};
  intent.strategy_id = StrategyId{1, StrategyName{"s"}};
  intent.request.account = AccountId{"A1"};
  intent.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTC-USDT"};
  intent.request.base_amount = D("1");
  intent.request.limit_price = D("100");
  ASSERT_TRUE(tracker.Register(intent).ok());
  auto unknown = tracker.MarkSubmissionUnknown(intent.client_id);
  ASSERT_TRUE(unknown.ok());
  EXPECT_EQ(unknown->snapshot.display_state,
            OrderDisplayState::SubmissionUnknown);
  ASSERT_EQ(tracker.ReconciliationQueue().size(), 1);
  OrderUpdate recovered;
  recovered.account = intent.request.account;
  recovered.market = intent.request.market;
  recovered.client_id = intent.client_id;
  recovered.exchange_order_id = ExchangeOrderId{"E1"};
  recovered.exchange_status = ExchangeOrderStatus::Open;
  auto reconciled = tracker.Reconcile(recovered);
  ASSERT_TRUE(reconciled.ok()) << reconciled.status();
  EXPECT_EQ(reconciled->reconciliation, ReconciliationState::Confirmed);
  EXPECT_EQ(reconciled->snapshot.display_state, OrderDisplayState::Open);
  ASSERT_EQ(reconciled->events.size(), 1);
  EXPECT_EQ(EventName(reconciled->events[0]), "OrderCreated");
  EXPECT_TRUE(tracker.ReconciliationQueue().empty());
  auto duplicate = tracker.ApplyOrderUpdate(recovered);
  ASSERT_TRUE(duplicate.ok());
  EXPECT_TRUE(duplicate->events.empty());
}

TEST(OrderTrackerTest, RoutesExchangeOnlyReportAndDoesNotRegressOnStaleStatus) {
  OrderTracker tracker;
  OrderIntent intent;
  intent.client_id = ClientOrderId{"B1"};
  intent.strategy_id = StrategyId{1, StrategyName{"s"}};
  intent.request.account = AccountId{"A1"};
  intent.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTC-USDT"};
  intent.request.base_amount = D("1");
  intent.request.limit_price = D("100");
  ASSERT_TRUE(tracker.Register(intent).ok());
  OrderUpdate update;
  update.account = intent.request.account;
  update.market = intent.request.market;
  update.client_id = intent.client_id;
  update.exchange_order_id = ExchangeOrderId{"E1"};
  update.exchange_status = ExchangeOrderStatus::Open;
  ASSERT_TRUE(tracker.ApplyOrderUpdate(update).ok());
  update.client_id.reset();
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
  OrderIntent intent;
  intent.client_id = ClientOrderId{"B1"};
  intent.strategy_id = StrategyId{1, StrategyName{"s"}};
  intent.request.account = AccountId{"A1"};
  intent.request.market =
      MarketId{ExchangeId{"simulated"}, InstrumentKind::Spot, "BTC-USDT"};
  intent.request.base_amount = D("1");
  intent.request.limit_price = D("100");
  ASSERT_TRUE(tracker.Register(intent).ok());
  auto failed =
      tracker.FailBeforeWrite(intent.client_id, "SendSlotUnavailable");
  ASSERT_TRUE(failed.ok()) << failed.status();
  EXPECT_EQ(failed->snapshot.display_state, OrderDisplayState::Failed);
  ASSERT_EQ(failed->events.size(), 1);
  EXPECT_EQ(EventName(failed->events[0]), "OrderFailed");
  EXPECT_EQ(std::get<OrderFailed>(failed->events[0]).reason,
            "SendSlotUnavailable");
  EXPECT_EQ(CodeOf(tracker.FailBeforeWrite(intent.client_id, "again").status()),
            ErrorCode::kOrderNotCancelable);
}

}  // namespace
}  // namespace hquant
