#pragma once

#include <cstdint>

#include "absl/status/statusor.h"
#include "hbot/base/ids.h"

namespace hbot::binance_spot {

struct DecodedClientId {
  uint64_t owner_key = 0;
  RunId run;
  ShardId shard_hint;
  uint32_t shard_sequence = 0;
};

// Candidate Binance Spot codec: H + 10/13/7 RFC4648 base32 digits. The owner
// key is stable; shard_hint is only a uniqueness/routing hint within one run.
// Enable live trading only after the target venue accepts and echoes this form.
absl::StatusOr<ClientOrderId> EncodeClientId(const OwnerId& owner, RunId run,
                                             ShardId shard,
                                             uint32_t shard_sequence);
absl::StatusOr<DecodedClientId> DecodeClientId(const ClientOrderId& id);

}  // namespace hbot::binance_spot
