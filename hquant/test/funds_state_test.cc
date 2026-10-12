#include "hquant/trading/funds_state.h"

#include <cstdlib>
#include <iostream>
#include <map>

namespace {

using namespace hquant::v1;

void Require(bool condition, const char* message) {
  if (condition) return;
  std::cerr << message << '\n';
  std::exit(1);
}

}  // namespace

int main() {
  FundsState funds({{"USDT", 100'000'000'000ULL}});
  const ClientOrderId a{"market-a"};
  const ClientOrderId b{"market-b"};
  Require(funds.Reserve(a, "USDT", 60'000'000'000ULL).ok(),
          "first reservation failed");
  Require(funds.Available("USDT") == 40'000'000'000ULL,
          "pending submit did not reserve funds");
  Require(!funds.Reserve(b, "USDT", 50'000'000'000ULL).ok(),
          "second symbol exceeded shared account funds");

  Require(funds.MarkVenueAccepted(a).ok(),
          "venue acceptance did not mark pending hold");
  Require(funds
              .ApplyBalance(BalanceUpdate{"USDT", 100'000'000'000ULL,
                                          40'000'000'000ULL, 60'000'000'000ULL})
              .ok(),
          "venue locked balance rejected");
  Require(funds.Available("USDT") == 40'000'000'000ULL,
          "confirmed venue lock was deducted twice");
  Require(funds.Reserve(b, "USDT", 30'000'000'000ULL).ok(),
          "second symbol could not use actual free funds");
  Require(funds.Available("USDT") == 10'000'000'000ULL,
          "second reservation not reflected");
  Require(funds.SetHold(a, 20'000'000'000ULL).ok(),
          "partial fill did not lower remaining hold");
  Require(!funds.SetHold(a, 70'000'000'000ULL).ok(),
          "remaining hold increased after fill");
  Require(funds.Release(b).ok(), "rejected order did not release hold");
  Require(funds.Available("USDT") == 40'000'000'000ULL,
          "released pending hold did not restore available funds");
  Require(funds.DebitUntilBalance("USDT", 15'000'000'000ULL).ok(),
          "fill debit could not be staged");
  Require(funds.Available("USDT") == 25'000'000'000ULL,
          "fill before balance update overstated free funds");
  Require(!funds.Reserve(b, "USDT", 30'000'000'000ULL).ok(),
          "stale balance admitted another order");
  Require(funds
              .ApplyBalance(BalanceUpdate{"USDT", 85'000'000'000ULL,
                                          65'000'000'000ULL, 20'000'000'000ULL})
              .ok(),
          "post-fill balance rejected");
  Require(funds.PendingDebit("USDT") == 0 &&
              funds.Available("USDT") == 65'000'000'000ULL,
          "ordered balance did not settle staged debit");
  Require(!funds.ApplyBalance(BalanceUpdate{"USDT", 100, 70, 40}).ok(),
          "inconsistent venue free and locked accepted");
  Require(
      funds
          .ApplyBalance(BalanceUpdate{"USDT", 85'000'000'000ULL,
                                      65'000'000'000ULL, 20'000'000'000ULL, 20})
          .ok(),
      "versioned balance rejected");
  Require(
      funds.IsStaleBalance(BalanceUpdate{
          "USDT", 90'000'000'000ULL, 70'000'000'000ULL, 20'000'000'000ULL, 19}),
      "older balance was not identified");
  Require(funds.ApplyBalance(BalanceUpdate{"USDT", 90'000'000'000ULL,
                                           70'000'000'000ULL, 20'000'000'000ULL,
                                           19})
                  .ok() &&
              funds.Asset("USDT")->total_nanos == 85'000'000'000ULL,
          "older balance replaced a newer snapshot");

  FundsState shared_locks({{"USDT", 100'000'000'000ULL},
                           {"BTC", 100'000'000'000ULL}});
  const ClientOrderId lock_a{"a"}, lock_b{"b"}, lock_c{"c"};
  Require(shared_locks.Reserve(lock_b, "USDT", 30'000'000'000ULL).ok() &&
              shared_locks.Reserve(lock_a, "USDT", 30'000'000'000ULL).ok() &&
              shared_locks.Reserve(lock_c, "BTC", 20'000'000'000ULL).ok() &&
              shared_locks.MarkVenueAccepted(lock_a).ok() &&
              shared_locks.MarkVenueAccepted(lock_b).ok() &&
              shared_locks.MarkVenueAccepted(lock_c).ok(),
          "shared-asset holds could not be prepared");
  Require(shared_locks.ApplyBalance(BalanceUpdate{
              "USDT", 100'000'000'000ULL, 50'000'000'000ULL,
              50'000'000'000ULL}).ok() &&
              shared_locks.Available("USDT") == 40'000'000'000ULL &&
              shared_locks.Available("BTC") == 80'000'000'000ULL,
          "shared lock attribution crossed assets or double counted");
  Require(shared_locks.SetHold(lock_a, 10'000'000'000ULL).ok() &&
              shared_locks.ApplyBalance(BalanceUpdate{
                  "USDT", 100'000'000'000ULL, 80'000'000'000ULL,
                  20'000'000'000ULL}).ok() &&
              shared_locks.Available("USDT") == 60'000'000'000ULL,
          "shrinking a covered hold did not reconcile the lock decrease");
  Require(shared_locks.Release(lock_a).ok() &&
              shared_locks.Available("USDT") == 70'000'000'000ULL &&
              shared_locks.ApplyBalance(BalanceUpdate{
                  "USDT", 100'000'000'000ULL, 70'000'000'000ULL,
                  30'000'000'000ULL}).ok() &&
              shared_locks.Available("USDT") == 70'000'000'000ULL,
          "remaining hold was not covered after a new venue lock");

  FundsState attributed(
      {{"BTC", 0}, {"USDT", 100'000'000'000ULL}, {"BNB", 1'000'000'000ULL}});
  const Fill third_asset_fee{{},
                             Side::Buy,
                             1,
                             100'000'000'000ULL,
                             100'000'000ULL,
                             10'000'000'000ULL,
                             10'000ULL,
                             MarketId{"BTCUSDT"},
                             ClientOrderId{"third-asset-fee"},
                             "BNB"};
  Require(attributed.RecordFillTotals(third_asset_fee, "BTC", "USDT").ok(),
          "configured third-asset fee was rejected");
  Require(
      attributed.MatchesProjectedTotal(BalanceUpdate{"BTC", 100'000'000ULL}) &&
          attributed.MatchesProjectedTotal(
              BalanceUpdate{"USDT", 90'000'000'000ULL}) &&
          attributed.MatchesProjectedTotal(
              BalanceUpdate{"BNB", 999'990'000ULL}),
      "third-asset fee was not attributed to tracked fill");
  Require(attributed.MatchesRecentProjectedTotal(
              BalanceUpdate{"USDT", 100'000'000'000ULL}) &&
              !attributed.MatchesProjectedTotal(
                  BalanceUpdate{"USDT", 100'000'000'000ULL}),
          "lagging private balance was not matched to a known fill prefix");
  attributed.ConfirmRecentProjectedTotal(
      BalanceUpdate{"USDT", 90'000'000'000ULL});
  Require(!attributed.MatchesRecentProjectedTotal(
              BalanceUpdate{"USDT", 100'000'000'000ULL}),
          "old projected balance remained valid after a newer observation");
  Require(!attributed.MatchesProjectedTotal(
              BalanceUpdate{"USDT", 89'000'000'000ULL}),
          "external balance debit matched tracked fills");
  Fill unconfigured_fee = third_asset_fee;
  unconfigured_fee.fee_asset = "OTHER";
  Require(!attributed.RecordFillTotals(unconfigured_fee, "BTC", "USDT").ok() &&
              attributed.MatchesProjectedTotal(
                  BalanceUpdate{"USDT", 90'000'000'000ULL}),
          "unconfigured fee asset changed projected totals");

  FundsState timed(
      {{"BTC", 0}, {"USDT", 100'000'000'000ULL}, {"BNB", 1'000'000'000ULL}});
  Fill later = third_asset_fee;
  later.transaction_ms = 300;
  Fill earlier = third_asset_fee;
  earlier.trade_id = 2;
  earlier.transaction_ms = 100;
  Require(timed.RecordFillTotals(later, "BTC", "USDT").ok() &&
              timed.RecordFillTotals(earlier, "BTC", "USDT").ok(),
          "out-of-order queried fills were rejected");
  Require(timed.MatchesProjectedAt(BalanceUpdate{"USDT", 90'000'000'000ULL,
                                                 90'000'000'000ULL, 0, 150}) &&
              !timed.MatchesProjectedAt(BalanceUpdate{
                  "USDT", 80'000'000'000ULL, 80'000'000'000ULL, 0, 150}) &&
              timed.MatchesProjectedAt(BalanceUpdate{
                  "USDT", 80'000'000'000ULL, 80'000'000'000ULL, 0, 350}),
          "balance projection ignored fill transaction times");
}
