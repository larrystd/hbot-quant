#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <variant>

#include "hbot/base/clock.h"
#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"
#include "hbot/model/market.h"
#include "hbot/storage/ports.h"

namespace hbot {

struct StopNewOrders {
  std::string reason;
};
struct CancelOwnedOrders {
  OwnerId owner;
};
struct RequestShardReport {};
using ShardCommandPayload =
    std::variant<StopNewOrders, CancelOwnedOrders, RequestShardReport>;

struct ShardCommand {
  uint64_t command_id = 0;
  ShardId target_shard;
  ShardCommandPayload payload;
  UtcTime issued_at{};
  MonoTime deadline{};
};

struct ShardReport {
  ShardId shard;
  uint64_t report_version = 0;
  UtcTime observed_at{};
  std::map<std::string, Decimal> lease_usage_by_asset;
  std::map<std::string, bool> market_readiness;
  std::map<std::string, bool> account_freshness;
  std::map<std::string, uint64_t> queue_watermarks;
  StorageHealth storage_health;
};

}  // namespace hbot
