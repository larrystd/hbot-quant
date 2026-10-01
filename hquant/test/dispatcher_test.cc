#include "service/dispatcher.h"

#include <chrono>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"

namespace {

hquant::Decimal D(const char* text) { return *hquant::Decimal::Parse(text); }

struct FakeRecorder final : hquant::RecorderPort {
  FakeRecorder(std::vector<std::string>* output, bool drop)
      : trace(output), drop_intent(drop) {}
  std::vector<std::string>* trace = nullptr;
  bool drop_intent = false;
  bool TryPush(hquant::RecordEnvelope record) override {
    const bool intent =
        std::holds_alternative<hquant::OrderIntent>(record.payload);
    trace->push_back(intent ? "intent" : "decision");
    return !(intent && drop_intent);
  }
};

struct FakeGateway final : hquant::OrderGateway {
  explicit FakeGateway(std::vector<std::string>* output) : trace(output) {}
  std::vector<std::string>* trace = nullptr;
  hquant::OrderIntent intent;
  absl::StatusOr<hquant::OrderIntent> PrepareSubmit(
      hquant::OrderCommand command) override {
    trace->push_back("prepare");
    intent.client_id = hquant::ClientOrderId("B1");
    intent.strategy_id = command.strategy_id;
    intent.request = command.request;
    return intent;
  }
  absl::Status StartPrepared(const hquant::ClientOrderId& id) override {
    trace->push_back("start");
    return id == hquant::ClientOrderId("B1")
               ? absl::OkStatus()
               : absl::InvalidArgumentError("wrong ID");
  }
  absl::Status AbortPrepared(const hquant::ClientOrderId&) override {
    trace->push_back("abort");
    return absl::OkStatus();
  }
  absl::Status StartCancel(const hquant::StrategyId&,
                           const hquant::ClientOrderId&) override {
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
  rule.min_notional = D("1");
  rule.revision = 1;
  rule.observed_at = now;
  hquant::RiskGate risk(
      {hquant::ShardId{0}, D("0"), std::chrono::seconds(300)});
  if (!risk.SetInitialLease({hquant::AccountId("A1"), hquant::AssetId("USDT"),
                             hquant::ShardId{0}, 1, D("100"),
                             now + std::chrono::hours(1)})
           .ok())
    return 1;
  hquant::StrategyId strategy_id{1, hquant::StrategyName("simple_pmm")};
  hquant::OrderRequest buy;
  buy.account = hquant::AccountId("A1");
  buy.market = market;
  buy.side = hquant::Side::Buy;
  buy.base_amount = D("0.01");
  buy.limit_price = D("100");
  hquant::ActionBatch batch;
  batch.ordered.emplace_back(
      hquant::CancelOrder{strategy_id, hquant::ClientOrderId("OLD")});
  batch.ordered.emplace_back(hquant::SubmitOrder{strategy_id, buy});
  std::vector<std::string> trace;
  FakeRecorder recorder{&trace, true};
  FakeGateway gateway{&trace};
  uint64_t sequence = 0;
  hquant::ActionDispatcher dispatcher(risk, gateway, recorder, hquant::RunId{1},
                                      hquant::ShardId{0}, sequence);
  hquant::DispatchContext context{strategy_id, hquant::DecisionId{1}, spec, rule,
                                  now,   hquant::MonoTime{},    true, true};
  auto results = dispatcher.Dispatch(batch, context);
  if (results.size() != 2 || !results[0].accepted || !results[1].accepted ||
      results[1].client_id != hquant::ClientOrderId("B1"))
    return 2;
  const std::vector<std::string> expected{"cancel", "decision", "prepare",
                                          "intent", "start",    "decision"};
  if (trace != expected || sequence != 3 ||
      dispatcher.local_gaps().size() != 1 ||
      dispatcher.local_gaps()[0].first_seq != 2)
    return 3;
  buy.limit_price = D("100.0549");
  hquant::ActionBatch quantized;
  quantized.ordered.emplace_back(hquant::SubmitOrder{strategy_id, buy});
  auto next = dispatcher.Dispatch(quantized, context);
  if (next.size() != 1 || !next[0].accepted || !next[0].intent ||
      !next[0].intent->request.limit_price ||
      *next[0].intent->request.limit_price->Compare(D("100.05")) != 0)
    return 4;
  context.market_live = false;
  auto rejected = dispatcher.Dispatch(quantized, context);
  if (rejected.size() != 1 || rejected[0].accepted ||
      rejected[0].reason != hquant::ErrorCode::kRiskNotReady ||
      !rejected[0].message.empty())
    return 5;
  context.market_live = true;
  buy.limit_price.reset();
  hquant::ActionBatch invalid;
  invalid.ordered.emplace_back(hquant::SubmitOrder{strategy_id, buy});
  rejected = dispatcher.Dispatch(invalid, context);
  if (rejected.size() != 1 || rejected[0].accepted ||
      rejected[0].reason != hquant::ErrorCode::kOrderInvalid ||
      !rejected[0].message.empty())
    return 6;
  return 0;
}
