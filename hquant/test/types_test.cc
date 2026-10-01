#include "base/types.h"

#include <string>

int RunDecimalChecks() {
  auto value = hquant::Decimal::Parse("1.2300");
  auto quantum = hquant::Decimal::Parse("0.05");
  if (!value.ok() || !quantum.ok() || value->ToString() != "1.23") return 1;
  auto down = value->Quantize(*quantum, hquant::RoundingMode::Down);
  auto up = value->Quantize(*quantum, hquant::RoundingMode::Up);
  if (!down.ok() || !up.ok() || down->ToString() != "1.2" ||
      up->ToString() != "1.25")
    return 2;
  auto negative = hquant::Decimal::Parse("-1.23");
  if (!negative.ok()) return 3;
  auto floor = negative->Quantize(*quantum, hquant::RoundingMode::Floor);
  if (!floor.ok() || floor->ToString() != "-1.25") return 4;
  for (const std::string& invalid :
       {"", "NaN", "Infinity", "silly", "12345678901234567890123456789"}) {
    if (hquant::Decimal::Parse(invalid).ok()) return 5;
  }
  auto zero = hquant::Decimal::Parse("0");
  if (!zero.ok() || value->Divide(*zero).ok()) return 6;
  auto sum = value->Add(*value);
  if (!sum.ok() || sum->ToString() != "2.46") return 7;
  return 0;
}

#include "base/market.h"

int RunDomainChecks() {
  hquant::StrategyId strategy_id{1, hquant::StrategyName("simple_pmm")};
  if (!strategy_id.IsValid()) return 1;
  strategy_id.value = uint64_t{1} << 48;
  if (strategy_id.IsValid() || hquant::RunId{0}.IsValid() ||
      hquant::ShardId{8}.IsValid())
    return 2;
  auto tick = hquant::Decimal::Parse("0.01");
  auto lot = hquant::Decimal::Parse("0.001");
  if (!tick.ok() || !lot.ok()) return 3;
  hquant::BookScale scale{*tick, *lot, 1};
  if (!scale.IsValid()) return 4;
  scale.base_per_lot = hquant::Decimal();
  if (scale.IsValid()) return 5;
  return 0;
}

int main() {
  if (RunDecimalChecks() != 0) return 1;
  return RunDomainChecks();
}
