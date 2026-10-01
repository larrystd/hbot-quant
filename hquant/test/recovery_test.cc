#include <unistd.h>

#include <chrono>
#include <deque>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/io_context.hpp"
#include "boost/asio/use_future.hpp"
#include "gtest/gtest.h"
#include "order/account_reports.h"
#include "order/order_gateway.h"
#include "order/order_tracker.h"
#include "order/risk.h"
#include "storage/history.h"
#include "storage/recorder.h"

namespace hquant {
namespace {

Decimal D(const char* text) { return *Decimal::Parse(text); }

class TemporaryDatabase {
 public:
  TemporaryDatabase() {
    path_ = (std::filesystem::temp_directory_path() /
             "hquant_recovery_integration_XXXXXX")
                .string();
    const int fd = mkstemp(path_.data());
    if (fd < 0) throw std::runtime_error("mkstemp failed");
    close(fd);
  }
  ~TemporaryDatabase() {
    std::filesystem::remove(path_);
    std::filesystem::remove(path_ + "-wal");
    std::filesystem::remove(path_ + "-shm");
  }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

class TestClock final : public Clock {
 public:
  UtcTime UtcNow() const override {
    return UtcTime(std::chrono::milliseconds(1'499'827'319'559));
  }
  MonoTime MonoNow() const override {
    return std::chrono::time_point_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now());
  }
};

class TimeoutTransport final : public HttpTransport {
 public:
  boost::asio::awaitable<absl::StatusOr<HttpResponse>> Send(
      HttpRequest request) override {
    ++calls;
    target = std::move(request.target);
    co_return absl::DeadlineExceededError("write outcome unknown");
  }
  int calls = 0;
  std::string target;
};

class FakeSignedRest final : public binance_spot::SignedRestClient {
 public:
  boost::asio::awaitable<absl::StatusOr<HttpResponse>> GetSigned(
      std::string path, std::chrono::steady_clock::time_point) override {
    targets.push_back(std::move(path));
    if (responses.empty()) co_return absl::UnavailableError("no response");
    auto result = std::move(responses.front());
    responses.pop_front();
    co_return result;
  }
  std::deque<HttpResponse> responses;
  std::vector<std::string> targets;
};

template <typename T>
T RunAsync(boost::asio::awaitable<T> operation) {
  boost::asio::io_context io;
  auto result =
      boost::asio::co_spawn(io, std::move(operation), boost::asio::use_future);
  io.run();
  return result.get();
}

TEST(RecoveryIntegrationTest, UnknownWriteAndCrashUseOriginalIdWithoutResend) {
  TemporaryDatabase database;
  TestClock clock;
  const AccountId account("A1");
  const MarketId market{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
  const MarketSpec spec{market, AssetId("BTC"), AssetId("USDT")};
  const StrategyId strategy_id{17, StrategyName("simple_pmm")};
  TradingRule rule{market,    D("0.01"), D("0.001"), D("0.001"),
                   D("0.01"), {},        1,          clock.UtcNow()};
  OrderRequest request;
  request.account = account;
  request.market = market;
  request.side = Side::Buy;
  request.type = OrderType::LimitMaker;
  request.base_amount = D("0.01");
  request.limit_price = D("100");
  RiskGate original_risk({ShardId{0}, D("0"), std::chrono::seconds(300)});
  ASSERT_TRUE(
      original_risk
          .SetInitialBudget({account, AssetId("USDT"), ShardId{0}, 1, D("100"),
                             clock.UtcNow() + std::chrono::hours(1)})
          .ok());
  auto hold = original_risk.TryHold(strategy_id, request, spec, rule,
                                    clock.UtcNow(), true, true);
  ASSERT_TRUE(hold.ok()) << hold.status();
  boost::asio::io_context io;
  TimeoutTransport transport;
  std::vector<binance_spot::GatewayEvent> events;
  binance_spot::BinanceGatewayConfig config;
  config.account = account;
  config.market = market;
  config.trading_rule = rule;
  config.run = RunId{42};
  config.shard = ShardId{0};
  config.api_key = "local-test-key";
  config.secret_key = "local-test-secret";
  binance_spot::BinanceOrderGateway gateway(
      io, transport, clock, config, [&](binance_spot::GatewayEvent event) {
        events.push_back(std::move(event));
      });
  ApprovedOrder approved{strategy_id, request, hold->hold_id, ActionBatchId{1},
                         clock.MonoNow() + std::chrono::seconds(1)};
  auto prepared = gateway.PrepareSubmit(std::move(approved));
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  ASSERT_TRUE(
      original_risk.AttachClientId(hold->hold_id, prepared->client_id).ok());
  auto recorder = SqliteHistoryWriter::Open(
      {database.path(), RunId{42}, clock.UtcNow(), 8, 1});
  ASSERT_TRUE(recorder.ok()) << recorder.status();
  HistoryRecord record;
  record.run_id = RunId{42};
  record.shard = ShardId{0};
  record.shard_sequence = 1;
  record.strategy_id = strategy_id;
  record.received_at_utc = clock.UtcNow();
  record.payload = *prepared;
  ASSERT_TRUE((*recorder)->TryPush(std::move(record)));
  ASSERT_TRUE((*recorder)->Flush().ok());
  OrderTracker original_tracker;
  ASSERT_TRUE(original_tracker.Register(*prepared).ok());
  ASSERT_TRUE(gateway.StartPrepared(prepared->client_id).ok());
  io.run();
  ASSERT_EQ(transport.calls, 1);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].kind, binance_spot::GatewayEventKind::SubmissionUnknown);
  ASSERT_TRUE(original_tracker.MarkSubmissionUnknown(prepared->client_id).ok());
  ASSERT_TRUE(original_risk.MarkSubmissionUnknown(hold->hold_id).ok());
  EXPECT_EQ(
      *original_risk.Available(account, AssetId("USDT"))->Compare(D("99")), 0);
  EXPECT_FALSE(gateway.StartPrepared(prepared->client_id).ok());
  EXPECT_EQ(transport.calls, 1);
  recorder->reset();  // abrupt exit: no clean-stop marker

  auto recovered = LoadRecoverySnapshot(database.path(), RunId{42});
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_TRUE(recovered->crash_tail_possible);
  EXPECT_TRUE(recovered->needs_reconciliation);
  ASSERT_EQ(recovered->context.recovered_prepared_orders.size(), 1);
  EXPECT_EQ(recovered->context.recovered_prepared_orders[0].client_id,
            prepared->client_id);
  binance_spot::RestartReconciliationInput restart;
  restart.account = account;
  restart.assigned_markets = {market};
  restart.persisted_prepared_orders =
      recovered->context.recovered_prepared_orders;
  restart.history_complete = recovered->manifest.history_complete;
  restart.previous_run_clean = !recovered->crash_tail_possible;
  restart.executor_checkpoints_complete = false;
  auto plan = binance_spot::PlanRestart(restart);
  ASSERT_TRUE(plan.ok()) << plan.status();
  ASSERT_EQ(plan->known_orders.size(), 1);
  EXPECT_EQ(plan->known_orders[0].original_client_id, prepared->client_id);
  EXPECT_FALSE(plan->markets_to_scan.empty());
  EXPECT_TRUE(plan->pause_stateful_executors);

  RiskGate restored_risk({ShardId{0}, D("0"), std::chrono::seconds(300)});
  ASSERT_TRUE(
      restored_risk
          .SetInitialBudget({account, AssetId("USDT"), ShardId{0}, 1, D("100"),
                             clock.UtcNow() + std::chrono::hours(1)})
          .ok());
  auto restored_hold = restored_risk.TryHold(strategy_id, request, spec, rule,
                                             clock.UtcNow(), true, true);
  ASSERT_TRUE(restored_hold.ok());
  ASSERT_TRUE(
      restored_risk.AttachClientId(restored_hold->hold_id, prepared->client_id)
          .ok());
  ASSERT_TRUE(restored_risk.MarkSubmissionUnknown(restored_hold->hold_id).ok());
  OrderTracker tracker;
  ASSERT_TRUE(
      tracker.Register(recovered->context.recovered_prepared_orders[0]).ok());
  ASSERT_TRUE(tracker.MarkSubmissionUnknown(prepared->client_id).ok());
  FakeSignedRest missing;
  missing.responses.push_back(
      {404, R"({"code":-2013,"msg":"Order does not exist."})"});
  binance_spot::ReconciliationClient missing_client(missing);
  auto unresolved = RunAsync(missing_client.Query(
      plan->known_orders[0], EventTime{{}, clock.UtcNow(), clock.MonoNow()},
      std::chrono::steady_clock::now() + std::chrono::seconds(2)));
  ASSERT_TRUE(unresolved.ok());
  EXPECT_FALSE(unresolved->complete);
  EXPECT_EQ(
      *restored_risk.Available(account, AssetId("USDT"))->Compare(D("99")), 0);

  FakeSignedRest rest;
  rest.responses.push_back(
      {200,
       "{\"symbol\":\"BTCUSDT\",\"clientOrderId\":\"" +
           prepared->client_id.value +
           "\",\"orderId\":123,\"status\":\"FILLED\",\"executedQty\":\"0.01\","
           "\"cummulativeQuoteQty\":\"1\",\"updateTime\":1499827319559}"});
  rest.responses.push_back(
      {200,
       R"([{"symbol":"BTCUSDT","id":7,"orderId":123,"price":"100","qty":"0.01","quoteQty":"1","commission":"0.00001","commissionAsset":"BTC","time":1499827319559,"isMaker":true}])"});
  binance_spot::ReconciliationClient client(rest);
  auto reconciled = RunAsync(client.Query(
      plan->known_orders[0], EventTime{{}, clock.UtcNow(), clock.MonoNow()},
      std::chrono::steady_clock::now() + std::chrono::seconds(2)));
  ASSERT_TRUE(reconciled.ok()) << reconciled.status();
  ASSERT_TRUE(reconciled->complete) << reconciled->unresolved_status;
  ASSERT_TRUE(reconciled->order);
  ASSERT_EQ(reconciled->trades.size(), 1);
  ASSERT_EQ(rest.targets.size(), 2);
  EXPECT_EQ(rest.targets[0], "/api/v3/order?symbol=BTCUSDT&origClientOrderId=" +
                                 prepared->client_id.value);
  ASSERT_TRUE(tracker.ApplyTradeUpdate(reconciled->trades[0]).ok());
  ASSERT_TRUE(tracker.ApplyTradeUpdate(reconciled->trades[0]).ok());
  auto final = tracker.Reconcile(*reconciled->order);
  ASSERT_TRUE(final.ok()) << final.status();
  EXPECT_EQ(final->snapshot.display_state, OrderDisplayState::Traded);
  EXPECT_EQ(*final->snapshot.cumulative_base.Compare(D("0.01")), 0);
  ASSERT_TRUE(
      restored_risk
          .ApplyTrade(restored_hold->hold_id, AssetId("USDT"), D("1"), D("0"))
          .ok());
  ASSERT_TRUE(restored_risk.Release(restored_hold->hold_id).ok());
  EXPECT_EQ(
      *restored_risk.Available(account, AssetId("USDT"))->Compare(D("99")), 0);
  EXPECT_EQ(transport.calls, 1);  // no second POST after restart
}

}  // namespace
}  // namespace hquant
