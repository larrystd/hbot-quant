#include "hquant/trading/order_manager.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace hquant::v1 {

absl::StatusOr<OrderId> OrderManager::NextLocalOrderId() {
  if (next_local_order_id_ == std::numeric_limits<uint64_t>::max()) {
    return absl::OutOfRangeError("local order id exhausted");
  }
  return OrderId{next_local_order_id_++};
}

absl::Status OrderManager::Begin(TrackedOrder order) {
  const auto id = order.client_order_id;
  const auto local = order.local_order_id;
  const auto shard = order.shard;
  if (local.value == 0 || local_ids_.contains(local.value)) {
    return absl::AlreadyExistsError("invalid or duplicate local order id");
  }
  if (auto status = state_.Begin(std::move(order)); !status.ok()) return status;
  local_ids_.emplace(local.value, id);
  active_order_ids_[shard.value].insert(id.value);
  next_local_order_id_ = std::max(next_local_order_id_, local.value + 1);
  return absl::OkStatus();
}

absl::Status OrderManager::AbortSubmit(const ClientOrderId& id) {
  const TrackedOrder* order = Find(id);
  if (!order) return absl::NotFoundError("unknown client order id");
  const auto local = order->local_order_id.value;
  const auto shard = order->shard.value;
  if (auto status = state_.AbortSubmit(id); !status.ok()) return status;
  local_ids_.erase(local);
  active_order_ids_[shard].erase(id.value);
  ErasePlan(id);
  return absl::OkStatus();
}

absl::Status OrderManager::BeginCancel(const ClientOrderId& id,
                                       uint64_t source_bench_seq,
                                       uint64_t requested_ms) {
  return state_.BeginCancel(id, source_bench_seq, requested_ms);
}

absl::Status OrderManager::AbortCancel(const ClientOrderId& id) {
  const auto status = state_.AbortCancel(id);
  if (status.ok()) RefreshPlan(id);
  return status;
}

absl::Status OrderManager::MarkSubmitUnknown(const ClientOrderId& id) {
  const auto status = state_.MarkSubmitUnknown(id);
  if (status.ok()) Arm(id);
  return status;
}

absl::Status OrderManager::MarkCancelUnknown(const ClientOrderId& id) {
  const auto status = state_.MarkCancelUnknown(id);
  if (status.ok()) Arm(id);
  return status;
}

absl::Status OrderManager::MarkCancelRejected(const ClientOrderId& id) {
  const auto status = state_.MarkCancelRejected(id);
  if (status.ok()) RefreshPlan(id);
  return status;
}

absl::Status OrderManager::MarkWriting(const ClientOrderId& id, bool cancel) {
  TrackedOrder* order = state_.MutableFind(id);
  if (!order) return absl::NotFoundError("unknown client order id");
  if (cancel) {
    order->cancel_writing = true;
  } else {
    order->submit_writing = true;
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> OrderManager::ApplyReport(const OrderReport& report) {
  auto applied = state_.ApplyReport(report);
  if (!applied.ok()) return applied;
  const auto id = report.order.client_order_id;
  const TrackedOrder* order = Find(id);
  if (order && order->Terminal()) {
    active_order_ids_[order->shard.value].erase(id.value);
  }
  RefreshPlan(id);
  return applied;
}

absl::StatusOr<bool> OrderManager::ApplyFill(const Fill& fill) {
  auto applied = state_.ApplyFill(fill);
  if (!applied.ok()) return applied;
  const TrackedOrder* order = Find(fill.client_order_id);
  if (order && order->Terminal()) {
    active_order_ids_[order->shard.value].erase(fill.client_order_id.value);
  }
  RefreshPlan(fill.client_order_id);
  return applied;
}

const TrackedOrder* OrderManager::FindLocal(OrderId id) const {
  const auto found = local_ids_.find(id.value);
  return found == local_ids_.end() ? nullptr : Find(found->second);
}

std::vector<OrderId> OrderManager::OpenFor(ShardId shard) const {
  std::vector<OrderId> result;
  const auto found = active_order_ids_.find(shard.value);
  if (found == active_order_ids_.end()) return result;
  for (const std::string& id : found->second) {
    const TrackedOrder* order = Find(ClientOrderId{id});
    if (order && !order->Terminal()) result.push_back(order->local_order_id);
  }
  return result;
}

bool OrderManager::PendingFor(ShardId shard) const {
  const auto found = active_order_ids_.find(shard.value);
  if (found == active_order_ids_.end()) return false;
  for (const std::string& id : found->second) {
    const TrackedOrder* order = Find(ClientOrderId{id});
    if (order && !order->Terminal() &&
        (order->status == OrderStatus::PendingSubmit || order->submit_unknown ||
         order->cancel == CancelPhase::Pending ||
         order->cancel == CancelPhase::Unknown)) {
      return true;
    }
  }
  return false;
}

void OrderManager::Arm(const ClientOrderId& id, Clock::time_point now) {
  const TrackedOrder* order = Find(id);
  if (!order || order->Terminal()) return;
  if (order->status != OrderStatus::PendingSubmit &&
      order->cancel != CancelPhase::Pending &&
      order->cancel != CancelPhase::Unknown && !order->submit_unknown) {
    ErasePlan(id);
    return;
  }
  auto [found, inserted] = queries_.try_emplace(id.value);
  if (inserted || !found->second.in_flight) {
    if (!inserted) due_queries_.erase({found->second.due, id.value});
    found->second.due = now + std::chrono::seconds(2);
    due_queries_.insert({found->second.due, id.value});
  }
}

void OrderManager::ErasePlan(const ClientOrderId& id) {
  const auto found = queries_.find(id.value);
  if (found == queries_.end()) return;
  if (!found->second.in_flight)
    due_queries_.erase({found->second.due, id.value});
  queries_.erase(found);
}

void OrderManager::RefreshPlan(const ClientOrderId& id) {
  const TrackedOrder* order = Find(id);
  if (!order || order->Terminal() ||
      (order->status != OrderStatus::PendingSubmit &&
       order->cancel != CancelPhase::Pending &&
       order->cancel != CancelPhase::Unknown && !order->submit_unknown)) {
    ErasePlan(id);
  }
}

void OrderManager::QueryFinished(const ClientOrderId& id) {
  const auto found = queries_.find(id.value);
  if (found != queries_.end() && found->second.in_flight) {
    found->second.in_flight = false;
    due_queries_.insert({found->second.due, id.value});
  }
  RefreshPlan(id);
}

void OrderManager::RearmPending(Clock::time_point now) {
  for (const auto& [id, order] : state_.Orders()) Arm(ClientOrderId{id}, now);
}

std::optional<OrderManager::Clock::time_point> OrderManager::NextDeadline()
    const {
  if (due_queries_.empty()) return std::nullopt;
  return due_queries_.begin()->first;
}

std::optional<QueryCommand> OrderManager::TakeDue(Clock::time_point now) {
  while (!due_queries_.empty() && due_queries_.begin()->first <= now) {
    const std::string id = due_queries_.begin()->second;
    due_queries_.erase(due_queries_.begin());
    const auto found = queries_.find(id);
    if (found == queries_.end()) continue;
    const TrackedOrder* order = Find(ClientOrderId{id});
    if (!order || order->Terminal()) {
      queries_.erase(found);
      continue;
    }
    QueryPlan& plan = found->second;
    plan.in_flight = true;
    const auto delay =
        std::chrono::seconds(1U << std::min<uint8_t>(plan.attempts, 5));
    plan.due = now + delay;
    if (plan.attempts < 5) ++plan.attempts;
    return QueryCommand{order->market, order->client_order_id,
                        order->exchange_order_id};
  }
  return std::nullopt;
}

bool OrderManager::RepeatedQuery(const ClientOrderId& id) const {
  const auto found = queries_.find(id.value);
  return found != queries_.end() && found->second.attempts >= 2;
}

}  // namespace hquant::v1
