#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/trading/trading_event.h"

namespace hquant::v1 {

enum class CancelPhase { None, Pending, Unknown, Rejected, Confirmed };

struct TrackedOrder {
  ClientOrderId client_order_id;
  ExchangeOrderId exchange_order_id;
  OrderId local_order_id;
  ShardId shard;
  MarketId market;
  Side side = Side::Buy;
  uint64_t price_nanos = 0;
  uint64_t quantity_nanos = 0;
  uint64_t filled_nanos = 0;
  uint64_t reported_remaining_nanos = 0;
  uint64_t source_bench_seq = 0;
  uint64_t cancel_source_bench_seq = 0;
  OrderStatus status = OrderStatus::PendingSubmit;
  CancelPhase cancel = CancelPhase::None;
  bool submit_unknown = false;
  bool submit_writing = false;
  bool cancel_writing = false;
  uint64_t cancel_requested_ms = 0;
  uint64_t last_report_time_ms = 0;
  std::set<uint64_t> trade_ids;

  bool Terminal() const {
    return status == OrderStatus::Filled || status == OrderStatus::Rejected ||
           status == OrderStatus::Cancelled;
  }
  uint64_t Remaining() const { return quantity_nanos - filled_nanos; }
};

// Pure order lifecycle logic. TradingAccount is its only writer.
class OrderState {
 public:
  absl::Status Begin(TrackedOrder order);
  absl::Status AbortSubmit(const ClientOrderId& id);
  absl::Status BeginCancel(const ClientOrderId& id,
                           uint64_t source_bench_seq = 0,
                           uint64_t requested_ms = 0);
  absl::Status AbortCancel(const ClientOrderId& id);
  absl::Status MarkSubmitUnknown(const ClientOrderId& id);
  absl::Status MarkCancelUnknown(const ClientOrderId& id);
  absl::Status MarkCancelRejected(const ClientOrderId& id);
  absl::StatusOr<bool> ApplyReport(const OrderReport& report);
  absl::StatusOr<bool> ApplyFill(const Fill& fill);
  const TrackedOrder* Find(const ClientOrderId& id) const;
  TrackedOrder* MutableFind(const ClientOrderId& id);
  const std::map<std::string, TrackedOrder>& Orders() const { return orders_; }

 private:
  std::map<std::string, TrackedOrder> orders_;
};

}  // namespace hquant::v1
