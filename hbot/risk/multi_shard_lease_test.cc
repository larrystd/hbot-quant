#include <chrono>
#include <cstdio>

#include "absl/status/status.h"
#include "hbot/risk/lease.h"
#include "hbot/risk/risk_gate.h"

namespace {

hbot::Decimal D(const char* text) { return *hbot::Decimal::Parse(text); }
bool Equal(const hbot::Decimal& actual, const char* expected) {
  const auto comparison = actual.Compare(D(expected));
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

int main() {
  using namespace hbot;
  const AccountId account("same-account");
  const AssetId base("BTC"), quote("USDT");
  const MarketId market{VenueId("binance"), InstrumentKind::Spot, "BTCUSDT"};
  const MarketSpec spec{market, base, quote};
  const UtcTime now(std::chrono::microseconds(10'000'000));
  const UtcTime expiry = now + std::chrono::hours(1);
  StaticRiskLeaseBook book;
  REQUIRE(book.SetConservativeLimit(account, quote, D("100")).ok());
  REQUIRE(!book.SetConservativeLimit(account, quote, D("200")).ok());
  REQUIRE(
      !book.GrantInitial({account, base, ShardId{0}, 1, D("1"), expiry}, now)
           .ok());

  const RiskLease first{account, quote, ShardId{0}, 1, D("60"), expiry};
  const RiskLease second{account, quote, ShardId{1}, 1, D("40"), expiry};
  REQUIRE(book.GrantInitial(first, now).ok());
  REQUIRE(
      book.GrantInitial({account, quote, ShardId{1}, 1, D("41"), expiry}, now)
          .code() == absl::StatusCode::kResourceExhausted);
  REQUIRE(Equal(*book.GrantedTotal(account, quote), "60"));
  REQUIRE(book.GrantInitial(second, now).ok());
  REQUIRE(Equal(*book.GrantedTotal(account, quote), "100"));
  REQUIRE(book.GrantInitial(first, now).code() ==
          absl::StatusCode::kAlreadyExists);
  REQUIRE(
      !book.GrantInitial({account, quote, ShardId{2}, 1, D("1"), expiry}, now)
           .ok());
  REQUIRE(Equal(*book.GrantedTotal(account, quote), "100"));
  REQUIRE(!book.GrantInitial({account, quote, ShardId{2}, 1, D("0"), now}, now)
               .ok());

  RiskLease renewed = first;
  renewed.lease_version = 2;
  renewed.valid_until_utc = expiry + std::chrono::hours(1);
  REQUIRE(!book.RenewAfterReconciliation(renewed, now, false).ok());
  RiskLease forged = renewed;
  forged.hard_limit = D("61");
  REQUIRE(!book.RenewAfterReconciliation(forged, now, true).ok());
  forged = renewed;
  forged.lease_version = 1;
  REQUIRE(!book.RenewAfterReconciliation(forged, now, true).ok());
  forged = renewed;
  forged.valid_until_utc = expiry;
  REQUIRE(!book.RenewAfterReconciliation(forged, expiry, true).ok());
  REQUIRE(book.RenewAfterReconciliation(renewed, expiry, true).ok());
  REQUIRE(book.FindLease(account, quote, ShardId{0})->lease_version == 2);
  REQUIRE(Equal(*book.GrantedTotal(account, quote), "100"));

  TradingRule rule;
  rule.market = market;
  rule.price_increment = D("0.01");
  rule.base_increment = D("0.001");
  rule.min_base_amount = D("0.001");
  rule.min_notional = D("1");
  rule.revision = 1;
  rule.observed_at = now;
  const OwnerId owner{1, StrategyId("simple_pmm"), std::nullopt};
  RiskGate gate0({ShardId{0}, D("0"), std::chrono::seconds(300)});
  RiskGate gate1({ShardId{1}, D("0"), std::chrono::seconds(300)});
  REQUIRE(gate0.SetInitialLease(first).ok());
  REQUIRE(gate1.SetInitialLease(second).ok());
  REQUIRE(!gate0.SetInitialLease(second).ok());

  OrderRequest request;
  request.account = account;
  request.market = market;
  request.side = Side::Buy;
  request.base_amount = D("0.6");
  request.limit_price = D("100");
  auto unknown = gate0.TryReserve(owner, request, spec, rule, now, true, true);
  REQUIRE(unknown.ok());
  REQUIRE(gate0.MarkSubmissionUnknown(unknown->reservation_id).ok());
  REQUIRE(Equal(*gate0.Available(account, quote), "0"));
  REQUIRE(gate0.TryReserve(owner, request, spec, rule, now, true, true)
              .status()
              .code() == absl::StatusCode::kResourceExhausted);

  request.base_amount = D("0.4");
  auto other_shard =
      gate1.TryReserve(owner, request, spec, rule, now, true, true);
  REQUIRE(other_shard.ok());
  REQUIRE(Equal(*gate1.Available(account, quote), "0"));
  REQUIRE(!gate1.TryReserve(owner, request, spec, rule, now, true, false).ok());
  REQUIRE(gate0.ConfirmTerminal(unknown->reservation_id).ok());
  REQUIRE(Equal(*gate0.Available(account, quote), "60"));
  rule.observed_at = expiry;
  REQUIRE(gate0.TryReserve(owner, request, spec, rule, expiry, true, true)
              .status()
              .code() == absl::StatusCode::kFailedPrecondition);
  REQUIRE(!gate0.RenewAfterReconciliation(renewed, expiry, false).ok());
  forged = renewed;
  forged.hard_limit = D("61");
  REQUIRE(!gate0.RenewAfterReconciliation(forged, expiry, true).ok());
  REQUIRE(gate0.RenewAfterReconciliation(renewed, expiry, true).ok());
  request.base_amount = D("0.6");
  auto new_order =
      gate0.TryReserve(owner, request, spec, rule, expiry, true, true);
  REQUIRE(new_order.ok() && new_order->lease_version == 2);
  REQUIRE(Equal(*gate0.Available(account, quote), "0"));
  return 0;
}
