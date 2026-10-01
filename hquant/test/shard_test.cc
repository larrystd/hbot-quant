#include "shard/shard.h"

#include <chrono>

int main() {
  hquant::ReplayClock clock(hquant::UtcTime{}, hquant::MonoTime{});
  if (!clock.Advance({100, 0}).ok() || !clock.Advance({100, 1}).ok() ||
      !clock.Advance({101, 0}).ok())
    return 1;
  if (clock.Advance({100, 2}).ok() || clock.Advance({101, 0}).ok()) return 2;
  if (clock.UtcNow().time_since_epoch() != std::chrono::microseconds(101) ||
      clock.MonoNow().time_since_epoch() != std::chrono::microseconds(101))
    return 3;
  return 0;
}
