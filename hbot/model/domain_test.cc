#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"
#include "hbot/model/market.h"

int main() {
  hbot::OwnerId owner{1, hbot::StrategyId("simple_pmm"), std::nullopt};
  if (!owner.IsValid()) return 1;
  owner.owner_key = uint64_t{1} << 48;
  if (owner.IsValid() || hbot::RunId{0}.IsValid() || hbot::ShardId{8}.IsValid())
    return 2;
  auto tick = hbot::Decimal::Parse("0.01");
  auto lot = hbot::Decimal::Parse("0.001");
  if (!tick.ok() || !lot.ok()) return 3;
  hbot::BookScale scale{*tick, *lot, 1};
  if (!scale.IsValid()) return 4;
  scale.base_per_lot = hbot::Decimal();
  if (scale.IsValid()) return 5;
  return 0;
}
