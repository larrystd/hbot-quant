#include "order/risk.h"

#include <chrono>
#include <cstdio>
#include <string_view>

#include "absl/status/status.h"
#include "base/error.h"

namespace {

hquant::Decimal D(const char* text) { return *hquant::Decimal::Parse(text); }

}  // namespace

int RiskGateTestMain() {
  const hquant::AccountId account("A1");
  const hquant::AssetId base("BTC"), quote("USDT");
  const hquant::MarketId market{hquant::ExchangeId("binance"),
                                hquant::InstrumentKind::Spot, "BTCUSDT"};
  const hquant::MarketSpec spec{market, base, quote};
  const hquant::UtcTime now(std::chrono::microseconds(10'000'000));
  hquant::TradingRule rule;
  rule.market = market;
  rule.price_increment = D("0.01");
  rule.base_increment = D("0.001");
  rule.min_base_amount = D("0.001");
  rule.min_notional = D("1");
  rule.revision = 1;
  rule.observed_at = now;
  hquant::RiskGate gate(
      {hquant::ShardId{0}, D("0.001"), std::chrono::seconds(300)});
  if (!gate.SetInitialBudget({account, quote, hquant::ShardId{0}, 1, D("100.1"),
                              now + std::chrono::hours(1)})
           .ok())
    return 1;
  if (!gate.SetInitialBudget({account, base, hquant::ShardId{0}, 1, D("1.1"),
                              now + std::chrono::hours(1)})
           .ok())
    return 2;

  hquant::StrategyId strategy_id{1, hquant::StrategyName("simple_pmm")};
  hquant::OrderRequest buy;
  buy.account = account;
  buy.market = market;
  buy.side = hquant::Side::Buy;
  buy.base_amount = D("1");
  buy.limit_price = D("100");
  hquant::FundsHold rejected;
  rejected.hold_id = hquant::HoldId{777};
  std::string_view detail;
  if (gate.TryHoldCode(strategy_id, buy, spec, rule, now, false, true,
                       &rejected,
                       &detail) != hquant::ErrorCode::kRiskMarketNotLive ||
      rejected.hold_id.value != 777 || detail.empty())
    return 13;
  auto first = gate.TryHold(strategy_id, buy, spec, rule, now, true, true);
  if (!first.ok() ||
      !gate.AttachClientId(first->hold_id, hquant::ClientOrderId("B1")).ok())
    return 3;
  if (gate.TryHold(strategy_id, buy, spec, rule, now, true, true).ok())
    return 4;
  if (!gate.MarkSubmissionUnknown(first->hold_id).ok() ||
      gate.TryHold(strategy_id, buy, spec, rule, now, true, true).ok())
    return 5;
  if (!gate.ApplyTrade(first->hold_id, quote, D("40"), D("60.06")).ok())
    return 6;
  auto available = gate.Available(account, quote);
  if (!available.ok() || available->ToString() != "0.04") return 7;
  if (!gate.Release(first->hold_id).ok()) return 8;
  available = gate.Available(account, quote);
  if (!available.ok() || available->ToString() != "60.1") return 9;

  buy.base_amount = D("0.0015");
  if (gate.TryHold(strategy_id, buy, spec, rule, now, true, true).ok())
    return 10;
  buy.base_amount = D("0.01");
  if (gate.TryHold(strategy_id, buy, spec, rule, now, false, true).ok())
    return 11;
  gate.EmergencyStop();
  if (gate.TryHold(strategy_id, buy, spec, rule, now, true, true).ok())
    return 12;
  return 0;
}

namespace {

hquant::Decimal BudgetD(const char* text) {
  return *hquant::Decimal::Parse(text);
}
bool Equal(const hquant::Decimal& actual, const char* expected) {
  const auto comparison = actual.Compare(BudgetD(expected));
  return comparison.ok() && *comparison == 0;
}

#define REQUIRE(condition)                                                   \
  do {                                                                       \
    if (!(condition)) {                                                      \
      std::fprintf(stderr, "failed at line %d: %s\n", __LINE__, #condition); \
      return 1;                                                              \
    }                                                                        \
  } while (false)

}  // namespace

int BudgetTestMain() {
  using namespace hquant;
  const AccountId account("same-account");
  const AssetId base("BTC"), quote("USDT");
  const MarketId market{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
  const MarketSpec spec{market, base, quote};
  const UtcTime now(std::chrono::microseconds(10'000'000));
  const UtcTime expiry = now + std::chrono::hours(1);
  RiskBudgetAllocator book;
  REQUIRE(book.SetConservativeLimit(account, quote, BudgetD("100")).ok());
  REQUIRE(!book.SetConservativeLimit(account, quote, BudgetD("200")).ok());
  REQUIRE(!book.GrantInitial(
                   {account, base, ShardId{0}, 1, BudgetD("1"), expiry}, now)
               .ok());

  const RiskBudget first{account, quote, ShardId{0}, 1, BudgetD("60"), expiry};
  const RiskBudget second{account, quote, ShardId{1}, 1, BudgetD("40"), expiry};
  REQUIRE(book.GrantInitial(first, now).ok());
  REQUIRE(CodeOf(book.GrantInitial(
              {account, quote, ShardId{1}, 1, BudgetD("41"), expiry}, now)) ==
          ErrorCode::kRiskConfigInvalid);
  REQUIRE(Equal(*book.GrantedTotal(account, quote), "60"));
  REQUIRE(book.GrantInitial(second, now).ok());
  REQUIRE(Equal(*book.GrantedTotal(account, quote), "100"));
  REQUIRE(CodeOf(book.GrantInitial(first, now)) ==
          ErrorCode::kRiskConfigInvalid);
  REQUIRE(!book.GrantInitial(
                   {account, quote, ShardId{2}, 1, BudgetD("1"), expiry}, now)
               .ok());
  REQUIRE(Equal(*book.GrantedTotal(account, quote), "100"));
  REQUIRE(!book.GrantInitial({account, quote, ShardId{2}, 1, BudgetD("0"), now},
                             now)
               .ok());

  RiskBudget renewed = first;
  renewed.budget_version = 2;
  renewed.valid_until_utc = expiry + std::chrono::hours(1);
  REQUIRE(!book.RenewAfterOrderQuery(renewed, now, false).ok());
  RiskBudget forged = renewed;
  forged.hard_limit = BudgetD("61");
  REQUIRE(!book.RenewAfterOrderQuery(forged, now, true).ok());
  forged = renewed;
  forged.budget_version = 1;
  REQUIRE(!book.RenewAfterOrderQuery(forged, now, true).ok());
  forged = renewed;
  forged.valid_until_utc = expiry;
  REQUIRE(!book.RenewAfterOrderQuery(forged, expiry, true).ok());
  REQUIRE(book.RenewAfterOrderQuery(renewed, expiry, true).ok());
  REQUIRE(book.FindBudget(account, quote, ShardId{0})->budget_version == 2);
  REQUIRE(Equal(*book.GrantedTotal(account, quote), "100"));

  TradingRule rule;
  rule.market = market;
  rule.price_increment = BudgetD("0.01");
  rule.base_increment = BudgetD("0.001");
  rule.min_base_amount = BudgetD("0.001");
  rule.min_notional = BudgetD("1");
  rule.revision = 1;
  rule.observed_at = now;
  const StrategyId strategy_id{1, StrategyName("simple_pmm")};
  RiskGate gate0({ShardId{0}, BudgetD("0"), std::chrono::seconds(300)});
  RiskGate gate1({ShardId{1}, BudgetD("0"), std::chrono::seconds(300)});
  REQUIRE(gate0.SetInitialBudget(first).ok());
  REQUIRE(gate1.SetInitialBudget(second).ok());
  REQUIRE(!gate0.SetInitialBudget(second).ok());

  OrderRequest request;
  request.account = account;
  request.market = market;
  request.side = Side::Buy;
  request.base_amount = BudgetD("0.6");
  request.limit_price = BudgetD("100");
  auto unknown =
      gate0.TryHold(strategy_id, request, spec, rule, now, true, true);
  REQUIRE(unknown.ok());
  REQUIRE(gate0.MarkSubmissionUnknown(unknown->hold_id).ok());
  REQUIRE(Equal(*gate0.Available(account, quote), "0"));
  REQUIRE(
      CodeOf(gate0.TryHold(strategy_id, request, spec, rule, now, true, true)
                 .status()) == ErrorCode::kRiskBudgetExhausted);

  request.base_amount = BudgetD("0.4");
  auto other_shard =
      gate1.TryHold(strategy_id, request, spec, rule, now, true, true);
  REQUIRE(other_shard.ok());
  REQUIRE(Equal(*gate1.Available(account, quote), "0"));
  REQUIRE(
      !gate1.TryHold(strategy_id, request, spec, rule, now, true, false).ok());
  REQUIRE(gate0.Release(unknown->hold_id).ok());
  REQUIRE(Equal(*gate0.Available(account, quote), "60"));
  rule.observed_at = expiry;
  REQUIRE(
      CodeOf(gate0.TryHold(strategy_id, request, spec, rule, expiry, true, true)
                 .status()) == ErrorCode::kRiskBudgetExpired);
  REQUIRE(!gate0.RenewAfterOrderQuery(renewed, expiry, false).ok());
  forged = renewed;
  forged.hard_limit = BudgetD("61");
  REQUIRE(!gate0.RenewAfterOrderQuery(forged, expiry, true).ok());
  REQUIRE(gate0.RenewAfterOrderQuery(renewed, expiry, true).ok());
  request.base_amount = BudgetD("0.6");
  auto new_order =
      gate0.TryHold(strategy_id, request, spec, rule, expiry, true, true);
  REQUIRE(new_order.ok() && new_order->budget_version == 2);
  REQUIRE(Equal(*gate0.Available(account, quote), "0"));
  return 0;
}

int main() {
  const int risk_result = RiskGateTestMain();
  return risk_result != 0 ? risk_result : BudgetTestMain();
}
