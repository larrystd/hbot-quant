#include "order/account_reports.h"

#include <chrono>
#include <deque>
#include <future>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "base/error.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/use_future.hpp"
#include "gtest/gtest.h"
#include "order/order_tracker.h"

namespace hquant::binance_spot {
namespace {

Decimal D(const char* raw) { return *Decimal::Parse(raw); }
bool Equal(const Decimal& value, const char* raw) {
  auto compared = value.Compare(D(raw));
  return compared.ok() && *compared == 0;
}

MarketId Market() {
  return {ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
}
EventTime Received() {
  EventTime time;
  time.receive_utc = UtcTime(std::chrono::milliseconds(2000));
  return time;
}

std::string Report(const char* execution, const char* status, const char* last,
                   const char* cumulative, const char* last_quote,
                   const char* traded_value, int trade_id = -1,
                   const char* client = "C1", const char* original = "",
                   const char* fee = "0", const char* fee_asset_json = "null") {
  return std::string("{\"e\":\"executionReport\",\"E\":1000,\"T\":1001,") +
         "\"s\":\"BTCUSDT\",\"c\":\"" + client + "\",\"C\":\"" + original +
         "\",\"i\":123,\"x\":\"" + execution + "\",\"X\":\"" + status +
         "\",\"t\":" + std::to_string(trade_id) + ",\"l\":\"" + last +
         "\",\"z\":\"" + cumulative + "\",\"L\":\"100\",\"Y\":\"" + last_quote +
         "\",\"Z\":\"" + traded_value + "\",\"n\":\"" + fee +
         "\",\"N\":" + fee_asset_json + ",\"m\":true}";
}

class FakeRest final : public SignedRestClient {
 public:
  std::deque<HttpResponse> responses;
  std::vector<std::string> targets;

  boost::asio::awaitable<absl::StatusOr<HttpResponse>> GetSigned(
      std::string target, std::chrono::steady_clock::time_point) override {
    targets.push_back(std::move(target));
    if (responses.empty()) {
      co_return absl::UnavailableError("missing mock response");
    }
    auto result = std::move(responses.front());
    responses.pop_front();
    co_return result;
  }
};

template <typename T>
T RunAsync(boost::asio::awaitable<T> operation) {
  boost::asio::io_context io;
  auto result =
      boost::asio::co_spawn(io, std::move(operation), boost::asio::use_future);
  io.run();
  return result.get();
}

OrderToQuery Target() {
  return {AccountId("A1"), Market(), ClientOrderId("C1")};
}

std::string OrderJson(const char* client = "C1", const char* status = "FILLED",
                      const char* filled = "1", const char* quote = "100") {
  return std::string("{\"symbol\":\"BTCUSDT\",\"clientOrderId\":\"") + client +
         "\",\"orderId\":123,\"status\":\"" + status + "\",\"executedQty\":\"" +
         filled + "\",\"cummulativeQuoteQty\":\"" + quote +
         "\",\"updateTime\":1001}";
}

std::string TradeJson(int trade_id, const char* amount = "1",
                      const char* quote = "100") {
  return std::string("{\"symbol\":\"BTCUSDT\",\"id\":") +
         std::to_string(trade_id) +
         ",\"orderId\":123,\"price\":\"100\",\"qty\":\"" + amount +
         "\",\"quoteQty\":\"" + quote +
         "\",\"commission\":\"0.01\",\"commissionAsset\":\"BNB\","
         "\"time\":1001,\"isMaker\":true}";
}

TEST(AccountStreamParserTest, EmitsTradeBeforeStatusAndTrackerDeduplicates) {
  AccountPushParser stream(AccountId("A1"), ExchangeId("binance"));
  OrderTracker tracker;
  PreparedOrder prepared;
  prepared.client_order_id = ClientOrderId("C1");
  prepared.strategy_id = StrategyId{1, StrategyName("simple_pmm")};
  prepared.request.account = AccountId("A1");
  prepared.request.market = Market();
  prepared.request.quantity = D("1");
  prepared.request.limit_price = D("100");
  prepared.created_at_utc = Received().receive_utc;
  ASSERT_TRUE(tracker.Register(prepared).ok());

  auto first = stream.Parse(Report("TRADE", "PARTIALLY_FILLED", "0.4", "0.4",
                                   "40", "40", 7, "C1", "", "0.01", "\"BNB\""),
                            Received());
  ASSERT_TRUE(first.ok()) << first.status();
  ASSERT_EQ(first->events.size(), 2);
  ASSERT_TRUE(std::holds_alternative<TradeUpdate>(first->events[0]));
  ASSERT_TRUE(std::holds_alternative<OrderUpdate>(first->events[1]));
  const auto& trade = std::get<TradeUpdate>(first->events[0]);
  EXPECT_EQ(trade.exchange_trade_id.value, "7");
  EXPECT_EQ(trade.fees[0].asset.value, "BNB");
  ASSERT_TRUE(trade.time.exchange_utc.has_value());
  EXPECT_EQ(trade.time.exchange_utc->time_since_epoch().count(), 1001000);
  auto fill_result = tracker.ApplyTradeUpdate(trade);
  ASSERT_TRUE(fill_result.ok()) << fill_result.status();
  EXPECT_EQ(fill_result->events.size(), 1);
  ASSERT_TRUE(
      tracker.ApplyOrderUpdate(std::get<OrderUpdate>(first->events[1])).ok());
  auto duplicate = tracker.ApplyTradeUpdate(trade);
  ASSERT_TRUE(duplicate.ok());
  EXPECT_TRUE(duplicate->events.empty());
  EXPECT_TRUE(
      Equal(tracker.Snapshot(ClientOrderId("C1"))->traded_quantity, "0.4"));

  auto stale =
      stream.Parse(Report("NEW", "NEW", "0", "0", "0", "0"), Received());
  ASSERT_TRUE(stale.ok());
  ASSERT_TRUE(
      tracker.ApplyOrderUpdate(std::get<OrderUpdate>(stale->events[0])).ok());
  EXPECT_EQ(tracker.Snapshot(ClientOrderId("C1"))->display_state,
            OrderDisplayState::PartiallyTraded);

  OrderUpdate filled = std::get<OrderUpdate>(first->events[1]);
  filled.exchange_status = ExchangeOrderStatus::Traded;
  filled.traded_quantity = D("1");
  filled.traded_value = D("100");
  auto awaiting = tracker.ApplyOrderUpdate(filled);
  ASSERT_TRUE(awaiting.ok());
  EXPECT_EQ(awaiting->snapshot.display_state,
            OrderDisplayState::AwaitingTrades);
  auto second = stream.Parse(
      Report("TRADE", "FILLED", "0.6", "1", "60", "100", 8), Received());
  ASSERT_TRUE(second.ok());
  auto completed =
      tracker.ApplyTradeUpdate(std::get<TradeUpdate>(second->events[0]));
  ASSERT_TRUE(completed.ok()) << completed.status();
  EXPECT_EQ(completed->events.size(), 2);
  EXPECT_EQ(completed->snapshot.display_state, OrderDisplayState::Traded);
}

TEST(AccountStreamParserTest, CancelUsesOriginalIdAndGapsRequestResync) {
  AccountPushParser stream(AccountId("A1"), ExchangeId("binance"));
  auto canceled = stream.Parse(Report("CANCELED", "CANCELED", "0", "0", "0",
                                      "0", -1, "cancel-request", "C1"),
                               Received());
  ASSERT_TRUE(canceled.ok());
  EXPECT_EQ(std::get<OrderUpdate>(canceled->events[0]).client_order_id->value, "C1");
  EXPECT_FALSE(stream
                   .Parse(Report("CANCELED", "CANCELED", "0", "0", "0", "0", -1,
                                 "cancel-request", ""),
                          Received())
                   .ok());
  auto position = stream.Parse(
      R"({"e":"outboundAccountPosition","E":1000,"B":[{"a":"BTC","f":"0.2","l":"0.3"}]})",
      Received());
  ASSERT_TRUE(position.ok());
  ASSERT_EQ(position->events.size(), 1);
  const auto& balance = std::get<BalanceUpdate>(position->events[0]);
  EXPECT_TRUE(Equal(balance.total, "0.5"));
  EXPECT_TRUE(Equal(balance.available, "0.2"));
  auto terminated =
      stream.Parse(R"({"e":"eventStreamTerminated","E":1000})", Received());
  ASSERT_TRUE(terminated.ok());
  EXPECT_TRUE(terminated->stream_terminated);
  EXPECT_TRUE(terminated->account_resync_required);
  auto delta = stream.Parse(
      R"({"e":"balanceUpdate","E":1000,"a":"BTC","d":"1"})", Received());
  ASSERT_TRUE(delta.ok());
  EXPECT_TRUE(delta->account_resync_required);
  EXPECT_TRUE(delta->events.empty());
  EXPECT_EQ(CodeOf(stream
                       .Parse(Report("TRADE", "FILLED", "1", "1", "100", "100",
                                     7, "C1", "", "0.1", "null"),
                              Received())
                       .status()),
            ErrorCode::kAccountMessageInvalid);
  EXPECT_EQ(CodeOf(stream
                       .Parse(Report("TRADE", "FILLED", "bad", "1", "100",
                                     "100", 7, "C1", "", "0.1", "\"BNB\""),
                              Received())
                       .status()),
            ErrorCode::kAccountMessageInvalid);
}

TEST(ReconciliationTest, InvalidRestDecimalRequiresReconciliation) {
  FakeRest rest;
  rest.responses.push_back({200, OrderJson("C1", "FILLED", "bad")});
  OrderQueryClient client(rest);
  auto result = RunAsync(
      client.QueryOrder(Target(), Received(), std::chrono::steady_clock::now()));
  ASSERT_TRUE(result.ok());
  EXPECT_FALSE(result->complete);
  EXPECT_EQ(CodeOf(result->unresolved_status),
            ErrorCode::kOrderQueryResponseInvalid);
}

TEST(ReconciliationTest, QueriesOriginalIdThenTradesAndComparesTotals) {
  FakeRest rest;
  rest.responses.push_back({200, OrderJson()});
  rest.responses.push_back({200, "[" + TradeJson(7) + "]"});
  OrderQueryClient client(rest);
  auto result = RunAsync(
      client.QueryOrder(Target(), Received(),
                   std::chrono::steady_clock::now() + std::chrono::seconds(2)));
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_TRUE(result->complete) << result->unresolved_status;
  EXPECT_TRUE(result->unresolved_status.ok());
  ASSERT_EQ(result->trades.size(), 1);
  EXPECT_EQ(result->trades[0].client_order_id->value, "C1");
  EXPECT_EQ(rest.targets.size(), 2);
  EXPECT_EQ(rest.targets[0],
            "/api/v3/order?symbol=BTCUSDT&origClientOrderId=C1");
  EXPECT_EQ(rest.targets[1],
            "/api/v3/myTrades?symbol=BTCUSDT&orderId=123&limit=1000");
}

TEST(ReconciliationTest, MissingOrMismatchedOrderNeverProvesNoWrite) {
  FakeRest missing;
  missing.responses.push_back(
      {404, R"({"code":-2013,"msg":"Order does not exist."})"});
  OrderQueryClient missing_client(missing);
  auto absent = RunAsync(missing_client.QueryOrder(
      Target(), Received(), std::chrono::steady_clock::now()));
  ASSERT_TRUE(absent.ok());
  EXPECT_FALSE(absent->complete);
  EXPECT_FALSE(absent->order.has_value());
  EXPECT_EQ(CodeOf(absent->unresolved_status),
            ErrorCode::kOrderQueryFailed);
  EXPECT_EQ(missing.targets.size(), 1);

  FakeRest mismatch;
  mismatch.responses.push_back({200, OrderJson("other-id")});
  OrderQueryClient mismatch_client(mismatch);
  auto wrong = RunAsync(mismatch_client.QueryOrder(
      Target(), Received(), std::chrono::steady_clock::now()));
  ASSERT_TRUE(wrong.ok());
  EXPECT_FALSE(wrong->complete);
  EXPECT_FALSE(wrong->order.has_value());
  EXPECT_EQ(CodeOf(wrong->unresolved_status),
            ErrorCode::kOrderQueryIdentityMismatch);
  EXPECT_EQ(mismatch.targets.size(), 1);

  FakeRest incomplete;
  incomplete.responses.push_back({200, OrderJson()});
  incomplete.responses.push_back({200, "[" + TradeJson(7, "0.5", "50") + "]"});
  OrderQueryClient incomplete_client(incomplete);
  auto gap = RunAsync(incomplete_client.QueryOrder(
      Target(), Received(), std::chrono::steady_clock::now()));
  ASSERT_TRUE(gap.ok());
  EXPECT_FALSE(gap->complete);
  EXPECT_FALSE(gap->order.has_value());
  EXPECT_EQ(gap->trades.size(), 1);
  EXPECT_EQ(CodeOf(gap->unresolved_status),
            ErrorCode::kOrderQueryIdentityMismatch);
}

TEST(ReconciliationTest, PagesTradesAndBoundsIncompletePages) {
  FakeRest rest;
  rest.responses.push_back({200, OrderJson()});
  rest.responses.push_back({200, "[" + TradeJson(7, "0.4", "40") + "]"});
  rest.responses.push_back({200, "[" + TradeJson(8, "0.6", "60") + "]"});
  rest.responses.push_back({200, "[]"});
  OrderQueryClient client(rest, {4, 1, 10000});
  auto result = RunAsync(
      client.QueryOrder(Target(), Received(), std::chrono::steady_clock::now()));
  ASSERT_TRUE(result.ok());
  EXPECT_TRUE(result->complete) << result->unresolved_status;
  EXPECT_EQ(result->trades.size(), 2);
  ASSERT_EQ(rest.targets.size(), 4);
  EXPECT_EQ(rest.targets[2],
            "/api/v3/myTrades?symbol=BTCUSDT&orderId=123&limit=1&fromId=8");

  FakeRest capped;
  capped.responses.push_back({200, OrderJson()});
  capped.responses.push_back({200, "[" + TradeJson(7) + "]"});
  OrderQueryClient capped_client(capped, {1, 1, 10000});
  auto unresolved = RunAsync(capped_client.QueryOrder(
      Target(), Received(), std::chrono::steady_clock::now()));
  ASSERT_TRUE(unresolved.ok());
  EXPECT_FALSE(unresolved->complete);
  EXPECT_EQ(CodeOf(unresolved->unresolved_status),
            ErrorCode::kOrderQueryResponseTooLarge);
  EXPECT_EQ(unresolved->unresolved_status.message(),
            "trade query hit page limit");

  FakeRest later_failure;
  later_failure.responses.push_back({200, OrderJson()});
  later_failure.responses.push_back(
      {200, "[" + TradeJson(7, "0.4", "40") + "]"});
  later_failure.responses.push_back({429, R"({"code":-1003})"});
  OrderQueryClient later_client(later_failure, {4, 1, 10000});
  auto partial = RunAsync(later_client.QueryOrder(Target(), Received(),
                                             std::chrono::steady_clock::now()));
  ASSERT_TRUE(partial.ok());
  EXPECT_FALSE(partial->complete);
  EXPECT_FALSE(partial->order.has_value());
  ASSERT_EQ(partial->trades.size(), 1);
  EXPECT_EQ(partial->trades[0].exchange_trade_id.value, "7");
}

TEST(ReconciliationTest, RestartPlanAndOpenOrderDiscovery) {
  StartupQueryInput input;
  input.account = AccountId("A1");
  input.assigned_markets = {Market()};
  input.history_complete = false;
  input.previous_run_clean = false;
  input.executor_checkpoints_complete = false;
  PreparedOrder prepared;
  prepared.client_order_id = ClientOrderId("C1");
  prepared.request.account = input.account;
  prepared.request.market = Market();
  input.persisted_prepared_orders.push_back(prepared);
  OrderSnapshot snapshot;
  snapshot.client_order_id = ClientOrderId("C1");
  snapshot.request = prepared.request;
  input.live_snapshots.push_back(snapshot);
  auto plan = PlanStartupQueries(input);
  ASSERT_TRUE(plan.ok()) << plan.status();
  EXPECT_EQ(plan->known_orders.size(), 1);
  EXPECT_EQ(plan->markets_to_scan.size(), 1);
  EXPECT_TRUE(plan->pause_stateful_executors);

  FakeRest rest;
  rest.responses.push_back(
      {200,
       R"([{"symbol":"BTCUSDT","clientOrderId":"C1"},{"symbol":"BTCUSDT","clientOrderId":"C2"}])"});
  OrderQueryClient client(rest);
  auto discovered = RunAsync(client.ListOpenOrders(
      input.account, Market(), std::chrono::steady_clock::now()));
  ASSERT_TRUE(discovered.ok()) << discovered.status();
  ASSERT_EQ(discovered->size(), 2);
  EXPECT_EQ((*discovered)[1].client_order_id.value, "C2");
  EXPECT_EQ(rest.targets[0], "/api/v3/openOrders?symbol=BTCUSDT");

  FakeRest recent_rest;
  recent_rest.responses.push_back(
      {200, R"([{"symbol":"BTCUSDT","clientOrderId":"C3"}])"});
  OrderQueryClient recent_client(recent_rest);
  auto recent = RunAsync(recent_client.ListRecentOrders(
      input.account, Market(), UtcTime(std::chrono::milliseconds(1000)),
      UtcTime(std::chrono::milliseconds(2000)),
      std::chrono::steady_clock::now()));
  ASSERT_TRUE(recent.ok()) << recent.status();
  ASSERT_EQ(recent->size(), 1);
  EXPECT_EQ((*recent)[0].client_order_id.value, "C3");
  EXPECT_EQ(recent_rest.targets[0],
            "/api/v3/"
            "allOrders?symbol=BTCUSDT&startTime=1000&endTime=2000&limit=1000");
}

}  // namespace
}  // namespace hquant::binance_spot
