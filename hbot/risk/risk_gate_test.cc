#include "hbot/risk/risk_gate.h"

#include <chrono>

namespace {

hbot::Decimal D(const char* text) { return *hbot::Decimal::Parse(text); }

}  // namespace

int main() {
  const hbot::AccountId account("A1");
  const hbot::AssetId base("BTC"), quote("USDT");
  const hbot::MarketId market{hbot::VenueId("binance"),
                              hbot::InstrumentKind::Spot, "BTCUSDT"};
  const hbot::MarketSpec spec{market, base, quote};
  const hbot::UtcTime now(std::chrono::microseconds(10'000'000));
  hbot::TradingRule rule;
  rule.market = market;
  rule.price_increment = D("0.01");
  rule.base_increment = D("0.001");
  rule.min_base_amount = D("0.001");
  rule.min_notional = D("1");
  rule.revision = 1;
  rule.observed_at = now;
  hbot::RiskGate gate(
      {hbot::ShardId{0}, D("0.001"), std::chrono::seconds(300)});
  if (!gate.SetInitialLease({account, quote, hbot::ShardId{0}, 1, D("100.1"),
                             now + std::chrono::hours(1)})
           .ok())
    return 1;
  if (!gate.SetInitialLease({account, base, hbot::ShardId{0}, 1, D("1.1"),
                             now + std::chrono::hours(1)})
           .ok())
    return 2;

  hbot::OwnerId owner{1, hbot::StrategyId("simple_pmm"), std::nullopt};
  hbot::OrderRequest buy;
  buy.account = account;
  buy.market = market;
  buy.side = hbot::Side::Buy;
  buy.base_amount = D("1");
  buy.limit_price = D("100");
  auto first = gate.TryReserve(owner, buy, spec, rule, now, true, true);
  if (!first.ok() ||
      !gate.AttachClientId(first->reservation_id, hbot::ClientOrderId("B1"))
           .ok())
    return 3;
  if (gate.TryReserve(owner, buy, spec, rule, now, true, true).ok()) return 4;
  if (!gate.MarkSubmissionUnknown(first->reservation_id).ok() ||
      gate.TryReserve(owner, buy, spec, rule, now, true, true).ok())
    return 5;
  if (!gate.ApplyFill(first->reservation_id, quote, D("40"), D("60.06")).ok())
    return 6;
  auto available = gate.Available(account, quote);
  if (!available.ok() || available->ToString() != "0.04") return 7;
  if (!gate.ConfirmTerminal(first->reservation_id).ok()) return 8;
  available = gate.Available(account, quote);
  if (!available.ok() || available->ToString() != "60.1") return 9;

  buy.base_amount = D("0.0015");
  if (gate.TryReserve(owner, buy, spec, rule, now, true, true).ok()) return 10;
  buy.base_amount = D("0.01");
  if (gate.TryReserve(owner, buy, spec, rule, now, false, true).ok()) return 11;
  gate.EmergencyStop();
  if (gate.TryReserve(owner, buy, spec, rule, now, true, true).ok()) return 12;
  return 0;
}
