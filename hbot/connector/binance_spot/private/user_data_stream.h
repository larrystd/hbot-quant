#pragma once

#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "hbot/event/events.h"

namespace hbot::binance_spot {

struct UserDataBatch {
  // For executionReport TRADE, the fill precedes its status update. The
  // OrderTracker handles duplicate trade IDs and either arrival order.
  std::vector<AccountEvent> events;
  bool stream_terminated = false;
  bool account_resync_required = false;
};

// Pure JSON adapter. Network subscription/reconnect and account-level routing
// belong to their owning components; this object holds no mutable order state.
class UserDataStream {
 public:
  UserDataStream(AccountId account, VenueId venue)
      : account_(std::move(account)), venue_(std::move(venue)) {}

  absl::StatusOr<UserDataBatch> Parse(std::string_view json,
                                      EventTime received) const;

 private:
  AccountId account_;
  VenueId venue_;
};

}  // namespace hbot::binance_spot
