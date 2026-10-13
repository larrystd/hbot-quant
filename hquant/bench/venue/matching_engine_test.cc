#include "hquant/bench/venue/matching_engine.h"

#include <cstdlib>
#include <iostream>

using hquant::bench::MatchingEngine;
using hquant::bench::Owner;
using hquant::bench::Side;

namespace {
void Check(bool condition, const char* message) {
  if (!condition) {
    std::cerr << message << '\n';
    std::exit(1);
  }
}
}  // namespace

int main() {
  constexpr int64_t unit = 1'000'000'000;
  MatchingEngine engine;
  auto ask1 = engine.Place("BTCUSDT", Side::Sell, Owner::External,
                           100 * unit, 1 * unit);
  auto ask2 = engine.Place("BTCUSDT", Side::Sell, Owner::External,
                           101 * unit, 2 * unit);
  auto passive = engine.Place("BTCUSDT", Side::Buy, Owner::Account,
                              99 * unit, 2 * unit);
  Check(passive.fills.empty(), "non-crossing buy filled");
  auto crossing = engine.Place("BTCUSDT", Side::Buy, Owner::Account,
                               101 * unit, 2 * unit);
  Check(crossing.fills.size() == 2, "crossing order did not sweep levels");
  Check(crossing.fills[0].maker_id == ask1.order_id &&
            crossing.fills[0].price_nanos == 100 * unit &&
            crossing.fills[1].maker_id == ask2.order_id &&
            crossing.fills[1].price_nanos == 101 * unit,
        "fill did not use resting price and price priority");
  Check(crossing.remaining_nanos == 0, "crossing order not fully filled");
  Check(engine.Cancel(passive.order_id) == 2 * unit,
        "resting order did not release remaining quantity");
  auto sell = engine.Place("BTCUSDT", Side::Sell, Owner::External,
                           99 * unit, 1 * unit);
  Check(sell.fills.empty(), "canceled buy filled");

  MatchingEngine fifo;
  auto first = fifo.Place("BTCUSDT", Side::Buy, Owner::External,
                          100 * unit, 1 * unit);
  auto second = fifo.Place("BTCUSDT", Side::Buy, Owner::Account,
                           100 * unit, 2 * unit);
  auto aggressor = fifo.Place("BTCUSDT", Side::Sell, Owner::External,
                              100 * unit, 2 * unit);
  Check(aggressor.fills.size() == 2 &&
            aggressor.fills[0].maker_id == first.order_id &&
            aggressor.fills[1].maker_id == second.order_id,
        "same-price queue did not use FIFO");
  Check(fifo.Find(second.order_id)->remaining_nanos == 1 * unit,
        "partial fill remaining quantity is wrong");
  Check(fifo.Find(second.order_id)->filled_nanos == 1 * unit,
        "partial fill executed quantity is wrong");
  auto bids = fifo.Levels("BTCUSDT", Side::Buy);
  Check(bids.size() == 1 && bids[0].quantity_nanos == 1 * unit,
        "public book differs from matching book");

  MatchingEngine cancellation;
  auto first_bid = cancellation.Place("BTCUSDT", Side::Buy, Owner::External,
                                      100 * unit, 3 * unit);
  auto second_bid = cancellation.Place("BTCUSDT", Side::Buy, Owner::Account,
                                       100 * unit, 2 * unit);
  auto other_symbol = cancellation.Place("ETHUSDT", Side::Buy,
                                         Owner::External, 100 * unit, unit);
  bids = cancellation.Levels("BTCUSDT", Side::Buy);
  Check(bids.size() == 1 && bids[0].quantity_nanos == 5 * unit,
        "same-price book quantities did not aggregate");
  Check(cancellation.Cancel(first_bid.order_id) == 3 * unit &&
            !cancellation.Cancel(first_bid.order_id),
        "cancellation did not release exactly once");
  bids = cancellation.Levels("BTCUSDT", Side::Buy);
  Check(bids.size() == 1 && bids[0].quantity_nanos == 2 * unit,
        "canceled quantity remains in public book");
  auto external_sell = cancellation.Place("BTCUSDT", Side::Sell,
                                          Owner::External, 100 * unit, unit);
  Check(external_sell.fills.size() == 1 &&
            external_sell.fills[0].maker_id == second_bid.order_id &&
            external_sell.fills[0].maker_owner == Owner::Account &&
            external_sell.fills[0].taker_owner == Owner::External,
        "external sweep did not identify the account maker");
  Check(cancellation.Cancel(second_bid.order_id) == unit &&
            cancellation.Levels("BTCUSDT", Side::Buy).empty() &&
            cancellation.Levels("ETHUSDT", Side::Buy).size() == 1 &&
            cancellation.Find(other_symbol.order_id)->remaining_nanos == unit,
        "cancel did not remove the empty level or isolated symbol changed");

  MatchingEngine public_only;
  public_only.Place("BTCUSDT", Side::Sell, Owner::External, 100 * unit, unit);
  auto external_cross = public_only.Place("BTCUSDT", Side::Buy,
                                           Owner::External, 100 * unit, unit);
  Check(external_cross.fills.size() == 1 &&
            external_cross.fills[0].maker_owner == Owner::External &&
            external_cross.fills[0].taker_owner == Owner::External,
        "external-only public trade was attributed to the account");

  std::cout << "matching engine tests passed\n";
}
