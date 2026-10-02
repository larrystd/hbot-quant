#include "shard/action_executor.h"

#include <chrono>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"

namespace {

hquant::Decimal D(const char* text) { return *hquant::Decimal::Parse(text); }

struct FakeRecorder final : hquant::OrderHistoryWriter {
  FakeRecorder(std::vector<std::string>* output, bool drop)
      : trace(output), drop_prepared(drop) {}
  std::vector<std::string>* trace = nullptr;
  bool drop_prepared = false;
  bool TryPush(hquant::OrderHistoryRecord record) override {
    const bool prepared = std::holds_alternative<hquant::Order>(record.payload);
    trace->push_back(prepared ? "prepared" : "decision");
    return !(prepared && drop_prepared);
  }
};

struct FakeGateway final : hquant::OrderGateway {
  explicit FakeGateway(std::vector<std::string>* output) : trace(output) {}
  std::vector<std::string>* trace = nullptr;
  hquant::Order prepared;
  int next_id = 1;
  absl::StatusOr<hquant::Order> PrepareSubmit(
      const hquant::SubmitOrder& request, const hquant::StrategyId& strategy_id,
      hquant::MonoTime) override {
    trace->push_back("prepare");
    prepared.client_order_id =
        hquant::ClientOrderId("B" + std::to_string(next_id++));
    prepared.strategy_id = strategy_id;
    prepared.account = hquant::AccountId("A1");
    prepared.market = hquant::MarketId{hquant::ExchangeId("binance"),
                                       hquant::InstrumentKind::Spot, "BTCUSDT"};
    prepared.side = request.side;
    prepared.quantity = request.quantity;
    prepared.price = request.price;
    prepared.time_in_force = request.time_in_force;
    return prepared;
  }
  absl::Status StartPrepared(const hquant::ClientOrderId& id) override {
    trace->push_back("start");
    return !id.value.empty() ? absl::OkStatus()
                             : absl::InvalidArgumentError("wrong ID");
  }
  absl::Status AbortPrepared(const hquant::ClientOrderId&) override {
    trace->push_back("abort");
    return absl::OkStatus();
  }
  absl::Status StartCancel(const hquant::ClientOrderId&) override {
    trace->push_back("cancel");
    return absl::OkStatus();
  }
};

}  // namespace

int main() {
  const hquant::UtcTime now(std::chrono::microseconds(1'000'000));
  const hquant::MarketId market{hquant::ExchangeId("binance"),
                                hquant::InstrumentKind::Spot, "BTCUSDT"};
  const hquant::MarketSpec spec{market, hquant::AssetId("BTC"),
                                hquant::AssetId("USDT")};
  hquant::TradingRule rule;
  rule.market = market;
  rule.price_increment = D("0.01");
  rule.base_increment = D("0.001");
  rule.min_base_amount = D("0.001");
  rule.min_order_value = D("1");
  rule.revision = 1;
  rule.observed_at = now;
  hquant::RiskGate risk(
      {hquant::ShardId{0}, D("0"), std::chrono::seconds(300)});
  if (!risk.SetInitialBudget({hquant::AccountId("A1"), hquant::AssetId("USDT"),
                              hquant::ShardId{0}, 1, D("100"),
                              now + std::chrono::hours(1)})
           .ok())
    return 1;
  hquant::StrategyId strategy_id{1, hquant::StrategyName("simple_pmm")};
  hquant::SubmitOrder buy;
  buy.side = hquant::Side::Buy;
  buy.quantity = D("0.01");
  buy.price = D("100");
  hquant::ActionBatch batch;
  batch.ordered.emplace_back(hquant::CancelOrder{hquant::ClientOrderId("OLD")});
  batch.ordered.emplace_back(buy);
  std::vector<std::string> trace;
  FakeRecorder recorder{&trace, true};
  FakeGateway gateway{&trace};
  hquant::OrderTracker tracker;
  hquant::Order old{hquant::ClientOrderId("OLD"),
                    strategy_id,
                    hquant::AccountId("A1"),
                    market,
                    hquant::Side::Buy,
                    D("0.01"),
                    D("100"),
                    hquant::TimeInForce::Gtc};
  if (!tracker.Add(old).ok()) return 7;
  uint64_t sequence = 0;
  hquant::ActionExecutor action_executor(risk, gateway, tracker, recorder,
                                         hquant::RunId{1}, hquant::ShardId{0},
                                         sequence);
  hquant::ActionContext context{strategy_id,
                                hquant::AccountId("A1"),
                                hquant::ActionBatchId{1},
                                spec,
                                rule,
                                now,
                                hquant::MonoTime{},
                                true,
                                true};
  auto results = action_executor.Execute(batch, context);
  if (results.size() != 2 || !results[0].accepted || !results[1].accepted ||
      results[1].client_order_id != hquant::ClientOrderId("B1"))
    return 2;
  const std::vector<std::string> expected{"cancel",   "decision", "prepare",
                                          "prepared", "start",    "decision"};
  if (trace != expected || sequence != 3 ||
      action_executor.local_gaps().size() != 1 ||
      action_executor.local_gaps()[0].first_seq != 2)
    return 3;
  buy.price = D("100.0549");
  hquant::ActionBatch quantized;
  quantized.ordered.emplace_back(buy);
  auto next = action_executor.Execute(quantized, context);
  if (next.size() != 1 || !next[0].accepted || !next[0].order ||
      *next[0].order->price.Compare(D("100.05")) != 0)
    return 4;
  context.market_live = false;
  auto rejected = action_executor.Execute(quantized, context);
  if (rejected.size() != 1 || rejected[0].accepted ||
      rejected[0].reason != hquant::ErrorCode::kRiskMarketNotLive ||
      !rejected[0].message.empty())
    return 5;
  context.market_live = true;
  buy.price = D("0");
  hquant::ActionBatch invalid;
  invalid.ordered.emplace_back(buy);
  rejected = action_executor.Execute(invalid, context);
  if (rejected.size() != 1 || rejected[0].accepted ||
      rejected[0].reason != hquant::ErrorCode::kOrderPriceOrAmountInvalid ||
      !rejected[0].message.empty())
    return 6;
  return 0;
}
