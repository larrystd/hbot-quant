#include "hquant/trading/order_state.h"

#include <algorithm>
#include <string>
#include <utility>

#include "absl/status/status.h"

namespace hquant::v1 {
namespace {

absl::Status Missing(const ClientOrderId& id) {
  return absl::NotFoundError("unknown client order id: " + id.value);
}

}  // namespace

absl::Status OrderState::Begin(TrackedOrder order) {
  if (order.client_order_id.value.empty() || order.market.symbol.empty() ||
      order.price_nanos == 0 || order.quantity_nanos == 0 ||
      order.status != OrderStatus::PendingSubmit) {
    return absl::InvalidArgumentError("invalid pending order");
  }
  order.reported_remaining_nanos = order.quantity_nanos;
  if (!orders_.emplace(order.client_order_id.value, std::move(order)).second) {
    return absl::AlreadyExistsError("duplicate client order id");
  }
  return absl::OkStatus();
}

const TrackedOrder* OrderState::Find(const ClientOrderId& id) const {
  const auto found = orders_.find(id.value);
  return found == orders_.end() ? nullptr : &found->second;
}

TrackedOrder* OrderState::MutableFind(const ClientOrderId& id) {
  const auto found = orders_.find(id.value);
  return found == orders_.end() ? nullptr : &found->second;
}

absl::Status OrderState::AbortSubmit(const ClientOrderId& id) {
  auto found = orders_.find(id.value);
  if (found == orders_.end()) return Missing(id);
  if (found->second.status != OrderStatus::PendingSubmit ||
      found->second.exchange_order_id.value != 0) {
    return absl::FailedPreconditionError("cannot abort sent order");
  }
  orders_.erase(found);
  return absl::OkStatus();
}

absl::Status OrderState::BeginCancel(const ClientOrderId& id,
                                     uint64_t source_bench_seq,
                                     uint64_t requested_ms) {
  auto found = orders_.find(id.value);
  if (found == orders_.end()) return Missing(id);
  TrackedOrder& order = found->second;
  if (order.Terminal() || order.cancel == CancelPhase::Pending ||
      order.cancel == CancelPhase::Unknown) {
    return absl::FailedPreconditionError("order cannot be cancelled again");
  }
  order.cancel = CancelPhase::Pending;
  order.cancel_writing = false;
  order.cancel_source_bench_seq = source_bench_seq;
  order.cancel_requested_ms = requested_ms;
  return absl::OkStatus();
}

absl::Status OrderState::AbortCancel(const ClientOrderId& id) {
  auto found = orders_.find(id.value);
  if (found == orders_.end()) return Missing(id);
  if (found->second.cancel != CancelPhase::Pending) {
    return absl::FailedPreconditionError("cannot abort sent cancel");
  }
  found->second.cancel = CancelPhase::None;
  found->second.cancel_source_bench_seq = 0;
  found->second.cancel_requested_ms = 0;
  return absl::OkStatus();
}

absl::Status OrderState::MarkSubmitUnknown(const ClientOrderId& id) {
  auto found = orders_.find(id.value);
  if (found == orders_.end()) return Missing(id);
  if (found->second.status == OrderStatus::PendingSubmit) {
    found->second.submit_unknown = true;
  }
  return absl::OkStatus();
}

absl::Status OrderState::MarkCancelUnknown(const ClientOrderId& id) {
  auto found = orders_.find(id.value);
  if (found == orders_.end()) return Missing(id);
  if (found->second.cancel == CancelPhase::Pending) {
    found->second.cancel = CancelPhase::Unknown;
  }
  return absl::OkStatus();
}

absl::Status OrderState::MarkCancelRejected(const ClientOrderId& id) {
  auto found = orders_.find(id.value);
  if (found == orders_.end()) return Missing(id);
  if (found->second.cancel == CancelPhase::Pending ||
      found->second.cancel == CancelPhase::Unknown) {
    found->second.cancel = CancelPhase::Rejected;
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> OrderState::ApplyReport(const OrderReport& report) {
  const ClientOrderId& id = report.order.client_order_id;
  auto found = orders_.find(id.value);
  if (found == orders_.end()) return Missing(id);
  TrackedOrder& order = found->second;
  if (report.order.market != order.market || report.order.side != order.side ||
      report.order.price_nanos != order.price_nanos ||
      report.order.quantity_nanos != order.quantity_nanos ||
      report.order.remaining_nanos > order.quantity_nanos) {
    return absl::DataLossError("order report contradicts local intent");
  }
  if (report.order.exchange_order_id.value != 0 &&
      order.exchange_order_id.value != 0 &&
      report.order.exchange_order_id != order.exchange_order_id) {
    return absl::DataLossError("exchange order id changed");
  }
  if (report.event_time_ms != 0 && order.last_report_time_ms != 0 &&
      report.event_time_ms < order.last_report_time_ms) {
    if (report.authoritative_query && order.cancel == CancelPhase::Unknown &&
        !order.Terminal() &&
        (report.state == OrderStatus::Accepted ||
         report.state == OrderStatus::PartiallyFilled) &&
        report.order.remaining_nanos == order.reported_remaining_nanos) {
      order.cancel = CancelPhase::Rejected;
      return true;
    }
    return false;
  }
  if (report.state == OrderStatus::PendingSubmit) {
    return absl::InvalidArgumentError("venue cannot report PendingSubmit");
  }
  if (order.Terminal() && report.state != order.status) {
    if (order.status != OrderStatus::Rejected &&
        (report.state == OrderStatus::Accepted ||
         report.state == OrderStatus::PartiallyFilled) &&
        report.order.remaining_nanos >= order.reported_remaining_nanos) {
      // REST can confirm a cancel after its fills have been applied while an
      // older nonterminal user-stream report is still waiting in the journal.
      return false;
    }
    return absl::DataLossError("terminal order status changed: " + id.value +
                               " local=" +
                               std::to_string(static_cast<int>(order.status)) +
                               " venue=" +
                               std::to_string(static_cast<int>(report.state)));
  }
  if (report.order.remaining_nanos > order.reported_remaining_nanos) {
    return absl::DataLossError("reported remaining quantity increased");
  }
  if (report.state == OrderStatus::Rejected && order.filled_nanos != 0) {
    return absl::DataLossError("filled order rejected");
  }
  if ((report.state == OrderStatus::Filled ||
       report.state == OrderStatus::Cancelled) &&
      report.order.remaining_nanos != order.Remaining()) {
    return absl::DataLossError("terminal report has unaccounted fills");
  }
  const OrderStatus previous = order.status;
  const CancelPhase previous_cancel = order.cancel;
  const ExchangeOrderId previous_exchange = order.exchange_order_id;
  const bool previous_unknown = order.submit_unknown;
  const uint64_t previous_remaining = order.reported_remaining_nanos;
  if (report.order.exchange_order_id.value != 0) {
    order.exchange_order_id = report.order.exchange_order_id;
  }
  order.submit_unknown = false;
  if (order.Terminal()) {
    // A duplicate terminal report may add the exchange ID or later quantity.
  } else if (report.state == OrderStatus::Rejected) {
    order.status = OrderStatus::Rejected;
  } else if (report.state == OrderStatus::Cancelled) {
    order.status = OrderStatus::Cancelled;
    order.cancel = CancelPhase::Confirmed;
  } else if (report.state == OrderStatus::Filled) {
    order.status = OrderStatus::Filled;
  } else if (report.state == OrderStatus::PartiallyFilled) {
    order.status = OrderStatus::PartiallyFilled;
  } else if (order.status == OrderStatus::PendingSubmit) {
    order.status = OrderStatus::Accepted;
  }
  if (order.cancel == CancelPhase::Unknown &&
      (report.state == OrderStatus::Accepted ||
       report.state == OrderStatus::PartiallyFilled) &&
      report.authoritative_query) {
    // A private fill or open report does not prove an in-flight cancel failed.
    // TradingAccount marks a spaced follow-up query authoritative.
    order.cancel = CancelPhase::Rejected;
  }
  if (report.order.remaining_nanos < order.reported_remaining_nanos) {
    order.reported_remaining_nanos = report.order.remaining_nanos;
  }
  order.last_report_time_ms =
      std::max(order.last_report_time_ms, report.event_time_ms);
  return previous != order.status || previous_cancel != order.cancel ||
         previous_exchange != order.exchange_order_id ||
         previous_unknown != order.submit_unknown ||
         previous_remaining != order.reported_remaining_nanos;
}

absl::StatusOr<bool> OrderState::ApplyFill(const Fill& fill) {
  auto found = orders_.find(fill.client_order_id.value);
  if (found == orders_.end()) return Missing(fill.client_order_id);
  TrackedOrder& order = found->second;
  if (order.trade_ids.contains(fill.trade_id)) return false;
  if (fill.market != order.market || fill.side != order.side ||
      fill.trade_id == 0 || fill.quantity_nanos == 0 ||
      fill.quantity_nanos > order.Remaining()) {
    return absl::DataLossError("fill contradicts local order");
  }
  if (!order.trade_ids.insert(fill.trade_id).second) return false;
  order.filled_nanos += fill.quantity_nanos;
  order.submit_unknown = false;
  if (order.status != OrderStatus::Cancelled) {
    order.status = order.Remaining() == 0 ? OrderStatus::Filled
                                          : OrderStatus::PartiallyFilled;
  }
  return true;
}

}  // namespace hquant::v1
