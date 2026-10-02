#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/order.h"

namespace hquant {

// Only the tracker interprets exchange reports as execution state. Its Order
// values remain stable for the life of the tracker. Returned pointers become
// invalid when the tracker is destroyed.
class OrderTracker {
 public:
  absl::Status Add(const Order& order);
  absl::StatusOr<UpdateResult> OnSendResult(const ClientOrderId& id,
                                            SendResult result);
  absl::StatusOr<UpdateResult> OnCancelRequested(const ClientOrderId& id);
  absl::StatusOr<UpdateResult> Apply(const OrderUpdate& update,
                                     ReportSource source);

  const Order* Find(const ClientOrderId& id) const;
  bool HasTrade(const ExchangeTradeId& id) const;
  std::vector<const Order*> ActiveOrders() const;
  std::vector<ClientOrderId> OrdersToQuery() const;

 private:
  enum class Status {
    PendingCreate,
    Open,
    PartiallyTraded,
    PendingCancel,
    SubmissionUnknown,
    NeedsQuery,
    AwaitingTrades,
    Traded,
    Canceled,
    Failed,
    Expired
  };
  struct Entry {
    Order order;
    Status status = Status::PendingCreate;
    Decimal executed_quantity;
    std::optional<Decimal> reported_quantity;
    std::optional<ExchangeOrderStatus> exchange_status;
  };

  static bool Finished(Status status);
  static bool Active(Status status);
  static Status TerminalStatus(ExchangeOrderStatus status);
  std::map<std::string, Entry> orders_;
  // Retain IDs of completed orders until a retention policy is defined. This
  // also catches a trade ID assigned to a different order in the same shard.
  std::map<std::string, std::string> seen_trades_;
};

}  // namespace hquant
