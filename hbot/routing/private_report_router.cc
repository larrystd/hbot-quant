#include "hbot/routing/private_report_router.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <variant>

#include "absl/status/status.h"

namespace hbot {

PrivateReportRouter::PrivateReportRouter(Options options,
                                         OrderOwnershipIndex& index)
    : options_(options), index_(index) {
  for (size_t shard = 0; shard < queues_.size(); ++shard) {
    if (shard != options_.reader_shard.value) {
      queues_[shard] =
          std::make_unique<Queue>(options_.queue_capacity_per_shard);
    }
  }
}

absl::StatusOr<std::unique_ptr<PrivateReportRouter>>
PrivateReportRouter::Create(Options options, OrderOwnershipIndex& index) {
  if (!options.reader_shard.IsValid() ||
      options.queue_capacity_per_shard == 0 ||
      options.quarantine_capacity == 0 ||
      options.queue_capacity_per_shard > 1'000'000 ||
      options.quarantine_capacity > 1'000'000) {
    return absl::InvalidArgumentError(
        "invalid private report router capacity/shard");
  }
  return std::unique_ptr<PrivateReportRouter>(
      new PrivateReportRouter(options, index));
}

RouteResult PrivateReportRouter::Quarantine(PrivateReport report,
                                            RouteResult result) {
  result.disposition = RouteDisposition::Quarantined;
  result.pause_account = true;
  result.request_reconcile = true;
  if (index_.KnowsAccount(result.account)) {
    paused_accounts_.insert(result.account.value);
  }
  if (quarantine_.size() >= options_.quarantine_capacity) {
    ++dropped_quarantine_count_;
    result.quarantine_dropped = true;
  } else {
    quarantine_.push_back(QuarantinedPrivateReport{
        std::move(report), result.failure, result.source_sequence});
  }
  return result;
}

RouteResult PrivateReportRouter::Route(PrivateReport report) {
  const AccountId& account = std::visit(
      [](const auto& value) -> const AccountId& { return value.account; },
      report);
  const MarketId& market = std::visit(
      [](const auto& value) -> const MarketId& { return value.market; },
      report);
  const std::optional<ClientOrderId>& client_id = std::visit(
      [](const auto& value) -> const std::optional<ClientOrderId>& {
        return value.client_id;
      },
      report);
  const std::optional<ExchangeOrderId>& exchange_id = std::visit(
      [](const auto& value) -> const std::optional<ExchangeOrderId>& {
        return value.exchange_order_id;
      },
      report);

  RouteResult result;
  result.account = account;
  if (next_source_sequence_ == std::numeric_limits<uint64_t>::max()) {
    result.failure = RouteFailure::InvalidReport;
    return Quarantine(std::move(report), std::move(result));
  }
  result.source_sequence = next_source_sequence_++;
  auto ownership = index_.Resolve(account, market, client_id, exchange_id);
  if (!ownership.ok()) {
    result.failure =
        ownership.status().code() == absl::StatusCode::kNotFound
            ? RouteFailure::UnknownOwnership
        : ownership.status().code() == absl::StatusCode::kInvalidArgument
            ? RouteFailure::InvalidReport
            : RouteFailure::ConflictingOwnership;
    return Quarantine(std::move(report), std::move(result));
  }
  RoutedPrivateReport routed{ownership->owner, ownership->current_shard,
                             result.source_sequence, std::move(report)};
  if (ownership->current_shard == options_.reader_shard) {
    result.disposition = RouteDisposition::Local;
    result.local_report = std::move(routed);
    return result;
  }
  auto& queue = *queues_[ownership->current_shard.value];
  const size_t tail = queue.tail.load(std::memory_order_relaxed);
  const size_t head = queue.head.load(std::memory_order_acquire);
  if (tail - head >= queue.slots.size()) {
    result.failure = RouteFailure::QueueFull;
    return Quarantine(std::move(routed.report), std::move(result));
  }
  queue.slots[tail % queue.slots.size()] = std::move(routed);
  queue.tail.store(tail + 1, std::memory_order_release);
  result.disposition = RouteDisposition::Forwarded;
  if (tail == head) result.wake_shard = ownership->current_shard;
  return result;
}

std::optional<RoutedPrivateReport> PrivateReportRouter::TryPop(
    ShardId target_shard) {
  if (!target_shard.IsValid() || target_shard == options_.reader_shard) {
    return std::nullopt;
  }
  auto& queue = *queues_[target_shard.value];
  const size_t head = queue.head.load(std::memory_order_relaxed);
  const size_t tail = queue.tail.load(std::memory_order_acquire);
  if (head == tail) return std::nullopt;
  auto result = std::move(queue.slots[head % queue.slots.size()]);
  queue.slots[head % queue.slots.size()].reset();
  queue.head.store(head + 1, std::memory_order_release);
  return result;
}

std::optional<QuarantinedPrivateReport>
PrivateReportRouter::TryPopQuarantined() {
  if (quarantine_.empty()) return std::nullopt;
  auto result = std::move(quarantine_.front());
  quarantine_.pop_front();
  return result;
}

bool PrivateReportRouter::IsAccountPaused(const AccountId& account) const {
  return paused_accounts_.contains(account.value);
}

void PrivateReportRouter::MarkReconciled(const AccountId& account) {
  paused_accounts_.erase(account.value);
}

}  // namespace hbot
