#include "hbot/base/decimal.h"

#include <string>

int main() {
  auto value = hbot::Decimal::Parse("1.2300");
  auto quantum = hbot::Decimal::Parse("0.05");
  if (!value.ok() || !quantum.ok() || value->ToString() != "1.23") return 1;
  auto down = value->Quantize(*quantum, hbot::RoundingMode::Down);
  auto up = value->Quantize(*quantum, hbot::RoundingMode::Up);
  if (!down.ok() || !up.ok() || down->ToString() != "1.2" ||
      up->ToString() != "1.25")
    return 2;
  auto negative = hbot::Decimal::Parse("-1.23");
  if (!negative.ok()) return 3;
  auto floor = negative->Quantize(*quantum, hbot::RoundingMode::Floor);
  if (!floor.ok() || floor->ToString() != "-1.25") return 4;
  for (const std::string& invalid :
       {"", "NaN", "Infinity", "silly", "12345678901234567890123456789"}) {
    if (hbot::Decimal::Parse(invalid).ok()) return 5;
  }
  auto zero = hbot::Decimal::Parse("0");
  if (!zero.ok() || value->Divide(*zero).ok()) return 6;
  auto sum = value->Add(*value);
  if (!sum.ok() || sum->ToString() != "2.46") return 7;
  return 0;
}
