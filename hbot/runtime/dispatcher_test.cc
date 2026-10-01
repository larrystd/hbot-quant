#include "hbot/runtime/dispatcher.h"

#include <chrono>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"

namespace {

hbot::Decimal D(const char* text) { return *hbot::Decimal::Parse(text); }

struct FakeRecorder final : hbot::RecorderPort {
  FakeRecorder(std::vector<std::string>* output, bool drop)
      : trace(output), drop_intent(drop) {}
  std::vector<std::string>* trace = nullptr;
  bool drop_intent = false;
  bool TryPush(hbot::RecordEnvelope record) override {
    const bool intent =
        std::holds_alternative<hbot::OrderIntent>(record.payload);
    trace->push_back(intent ? "intent" : "decision");
    return !(intent && drop_intent);
  }
};

struct FakeGateway final : hbot::OrderGateway {
  explicit FakeGateway(std::vector<std::string>* output) : trace(output) {}
  std::vector<std::string>* trace = nullptr;
  hbot::OrderIntent intent;
  absl::StatusOr<hbot::OrderIntent> PrepareSubmit(
      hbot::OrderCommand command) override {
    trace->push_back("prepare");
    intent.client_id = hbot::ClientOrderId("B1");
    intent.owner = command.owner;
    intent.request = command.request;
    return intent;
  }
  absl::Status StartPrepared(const hbot::ClientOrderId& id) override {
    trace->push_back("start");
    return id == hbot::ClientOrderId("B1")
               ? absl::OkStatus()
               : absl::InvalidArgumentError("wrong ID");
  }
  absl::Status AbortPrepared(const hbot::ClientOrderId&) override {
    trace->push_back("abort");
    return absl::OkStatus();
  }
  absl::Status StartCancel(const hbot::OwnerId&,
                           const hbot::ClientOrderId&) override {
    trace->push_back("cancel");
    return absl::OkStatus();
  }
};

}  // namespace

int main() {
  const hbot::UtcTime now(std::chrono::microseconds(1'000'000));
  const hbot::MarketId market{hbot::VenueId("binance"),
                              hbot::InstrumentKind::Spot, "BTCUSDT"};
  const hbot::MarketSpec spec{market, hbot::AssetId("BTC"),
                              hbot::AssetId("USDT")};
  hbot::TradingRule rule;
  rule.market = market;
  rule.price_increment = D("0.01");
  rule.base_increment = D("0.001");
  rule.min_base_amount = D("0.001");
  rule.min_notional = D("1");
  rule.revision = 1;
  rule.observed_at = now;
  hbot::RiskGate risk({hbot::ShardId{0}, D("0"), std::chrono::seconds(300)});
  if (!risk.SetInitialLease({hbot::AccountId("A1"), hbot::AssetId("USDT"),
                             hbot::ShardId{0}, 1, D("100"),
                             now + std::chrono::hours(1)})
           .ok())
    return 1;
  hbot::OwnerId owner{1, hbot::StrategyId("simple_pmm"), std::nullopt};
  hbot::OrderRequest buy;
  buy.account = hbot::AccountId("A1");
  buy.market = market;
  buy.side = hbot::Side::Buy;
  buy.base_amount = D("0.01");
  buy.limit_price = D("100");
  hbot::ActionBatch batch;
  batch.ordered.emplace_back(
      hbot::CancelOrder{owner, hbot::ClientOrderId("OLD")});
  batch.ordered.emplace_back(hbot::SubmitOrder{owner, buy});
  std::vector<std::string> trace;
  FakeRecorder recorder{&trace, true};
  FakeGateway gateway{&trace};
  uint64_t sequence = 0;
  hbot::ActionDispatcher dispatcher(risk, gateway, recorder, hbot::RunId{1},
                                    hbot::ShardId{0}, sequence);
  hbot::DispatchContext context{owner, hbot::DecisionId{1}, spec, rule,
                                now,   hbot::MonoTime{},    true, true};
  auto results = dispatcher.Dispatch(batch, context);
  if (results.size() != 2 || !results[0].accepted || !results[1].accepted ||
      results[1].client_id != hbot::ClientOrderId("B1"))
    return 2;
  const std::vector<std::string> expected{"cancel", "decision", "prepare",
                                          "intent", "start",    "decision"};
  if (trace != expected || sequence != 3 ||
      dispatcher.local_gaps().size() != 1 ||
      dispatcher.local_gaps()[0].first_seq != 2)
    return 3;
  buy.limit_price = D("100.0549");
  hbot::ActionBatch quantized;
  quantized.ordered.emplace_back(hbot::SubmitOrder{owner, buy});
  auto next = dispatcher.Dispatch(quantized, context);
  if (next.size() != 1 || !next[0].accepted || !next[0].intent ||
      !next[0].intent->request.limit_price ||
      *next[0].intent->request.limit_price->Compare(D("100.05")) != 0)
    return 4;
  return 0;
}
