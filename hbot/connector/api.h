#pragma once

#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"
#include "hbot/event/events.h"
#include "hbot/model/order.h"

namespace hbot {

// Methods are called only by the owning shard. Preparing reserves a bounded
// send slot, fixes the client ID, and registers PendingCreate, but sends no
// bytes. The caller attaches the RiskReservation and attempts to record the
// OrderIntent before StartPrepared. A failed StartPrepared proves no write was
// initiated; uncertain write outcomes arrive later as account events.
class OrderGateway {
 public:
  virtual ~OrderGateway() = default;
  virtual absl::StatusOr<OrderIntent> PrepareSubmit(OrderCommand command) = 0;
  virtual absl::Status StartPrepared(const ClientOrderId& client_id) = 0;
  virtual absl::Status AbortPrepared(const ClientOrderId& client_id) = 0;
  virtual absl::Status StartCancel(const OwnerId& owner,
                                   const ClientOrderId& client_id) = 0;
};

// Local Paper venue port used by replay and public-market Paper shards. The
// runtime depends on this interface; app selects the concrete PaperConnector.
class SimulatedVenue : public OrderGateway {
 public:
  virtual absl::Status OnBookBbo(const Decimal& bid, const Decimal& ask) = 0;
  virtual absl::Status OnPublicTrade(Side aggressor, const Decimal& price,
                                     const Decimal& public_amount) = 0;
  virtual Decimal BalanceOf(const AssetId& asset) const = 0;
  virtual Decimal AvailableBalance(const AssetId& asset) const = 0;
  virtual std::vector<AccountEvent> DrainEvents() = 0;
};

}  // namespace hbot
