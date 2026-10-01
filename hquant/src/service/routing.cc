#include "service/routing.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "absl/status/status.h"

namespace hquant {

OrderOwnershipIndex::OrderOwnershipIndex(DecodeOwnerKey decode_owner_key)
    : decode_owner_key_(std::move(decode_owner_key)) {}

OrderOwnershipIndex::ExchangeKey OrderOwnershipIndex::Key(
    const AccountId& account, const MarketId& market,
    const ExchangeOrderId& exchange_id) {
  return {account.value, market.exchange.value,
          static_cast<int>(market.instrument_kind), market.native_symbol,
          exchange_id.value};
}

absl::Status OrderOwnershipIndex::SetOwnerRoute(OwnerId owner,
                                                AccountId account,
                                                ShardId shard) {
  if (!owner.IsValid() || account.value.empty() || !shard.IsValid()) {
    return Error(ErrorCode::kRouteConfigInvalid, "invalid owner route");
  }
  auto found = owners_.find(owner.owner_key);
  if (found != owners_.end()) {
    if (found->second.owner != owner || found->second.account != account) {
      return Error(ErrorCode::kRouteConfigInvalid,
                   "owner key identity conflict");
    }
    found->second.shard = shard;
  } else {
    owners_.emplace(owner.owner_key,
                    OwnerRoute{std::move(owner), std::move(account), shard});
  }
  return absl::OkStatus();
}

absl::Status OrderOwnershipIndex::RegisterClient(ClientOrderId client_id,
                                                 AccountId account,
                                                 MarketId market,
                                                 const OwnerId& owner) {
  if (client_id.value.empty() || account.value.empty() ||
      market.exchange.value.empty() || market.native_symbol.empty()) {
    return Error(ErrorCode::kRouteReportInvalid,
                 "invalid client ownership key");
  }
  const auto route = owners_.find(owner.owner_key);
  if (route == owners_.end() || route->second.owner != owner ||
      route->second.account != account) {
    return Error(ErrorCode::kRouteOwnerUnknown,
                 "client owner has no matching route");
  }
  if (decode_owner_key_) {
    auto decoded = decode_owner_key_(client_id);
    if (decoded && *decoded != owner.owner_key) {
      return Error(ErrorCode::kRouteOwnershipConflict,
                   "client ID owner conflicts with registration");
    }
  }
  auto [it, inserted] = clients_.emplace(
      client_id.value, ClientMapping{account, market, owner.owner_key});
  if (!inserted &&
      (it->second.account != account || it->second.market != market ||
       it->second.owner_key != owner.owner_key)) {
    return Error(ErrorCode::kRouteOwnershipConflict,
                 "client ID ownership conflict");
  }
  return absl::OkStatus();
}

absl::Status OrderOwnershipIndex::RegisterExchange(
    AccountId account, MarketId market, ExchangeOrderId exchange_id,
    const OwnerId& owner, std::optional<ClientOrderId> client_id) {
  if (exchange_id.value.empty() || account.value.empty() ||
      market.exchange.value.empty() || market.native_symbol.empty()) {
    return Error(ErrorCode::kRouteReportInvalid,
                 "invalid exchange ownership key");
  }
  const auto route = owners_.find(owner.owner_key);
  if (route == owners_.end() || route->second.owner != owner ||
      route->second.account != account) {
    return Error(ErrorCode::kRouteOwnerUnknown,
                 "exchange owner has no matching route");
  }
  if (client_id) {
    auto resolved = Resolve(account, market, client_id, std::nullopt);
    if (!resolved.ok() || resolved->owner != owner) {
      return Error(ErrorCode::kRouteOwnershipConflict,
                   "exchange/client ownership conflict");
    }
  }
  auto [it, inserted] =
      exchanges_.emplace(Key(account, market, exchange_id), owner.owner_key);
  if (!inserted && it->second != owner.owner_key) {
    return Error(ErrorCode::kRouteOwnershipConflict,
                 "exchange ID ownership conflict");
  }
  return absl::OkStatus();
}

absl::StatusOr<OrderOwnership> OrderOwnershipIndex::Resolve(
    const AccountId& account, const MarketId& market,
    const std::optional<ClientOrderId>& client_id,
    const std::optional<ExchangeOrderId>& exchange_id) const {
  if (account.value.empty() || market.exchange.value.empty() ||
      market.native_symbol.empty() || (client_id && client_id->value.empty()) ||
      (exchange_id && exchange_id->value.empty())) {
    return Error(ErrorCode::kRouteReportInvalid,
                 "invalid private report identity");
  }
  std::optional<uint64_t> candidate;
  const auto merge = [&](uint64_t owner_key) -> absl::Status {
    if (candidate && *candidate != owner_key) {
      return Error(ErrorCode::kRouteOwnershipConflict,
                   "private report ownership conflict");
    }
    candidate = owner_key;
    return absl::OkStatus();
  };
  if (client_id && decode_owner_key_) {
    auto decoded = decode_owner_key_(*client_id);
    if (decoded) {
      auto status = merge(*decoded);
      if (!status.ok()) return status;
    }
  }
  if (client_id) {
    const auto found = clients_.find(client_id->value);
    if (found != clients_.end()) {
      if (found->second.account != account || found->second.market != market) {
        return Error(ErrorCode::kRouteOwnershipConflict,
                     "client ID account/market conflict");
      }
      auto status = merge(found->second.owner_key);
      if (!status.ok()) return status;
    }
  }
  if (exchange_id) {
    const auto found = exchanges_.find(Key(account, market, *exchange_id));
    if (found != exchanges_.end()) {
      auto status = merge(found->second);
      if (!status.ok()) return status;
    }
  }
  if (!candidate)
    return Error(ErrorCode::kRouteOwnerUnknown, "private report owner unknown");
  const auto route = owners_.find(*candidate);
  if (route == owners_.end())
    return Error(ErrorCode::kRouteOwnerUnknown, "owner route unknown");
  if (route->second.account != account) {
    return Error(ErrorCode::kRouteOwnershipConflict, "owner/account conflict");
  }
  return OrderOwnership{route->second.owner, route->second.shard};
}

bool OrderOwnershipIndex::KnowsAccount(const AccountId& account) const {
  for (const auto& [key, route] : owners_) {
    if (route.account == account) return true;
  }
  return false;
}

}  // namespace hquant

namespace hquant {

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
    return Error(ErrorCode::kRouteConfigInvalid,
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
    result.failure = ErrorCode::kRouteReportInvalid;
    return Quarantine(std::move(report), std::move(result));
  }
  result.source_sequence = next_source_sequence_++;
  auto ownership = index_.Resolve(account, market, client_id, exchange_id);
  if (!ownership.ok()) {
    result.failure = CodeOf(ownership.status());
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
    result.failure = ErrorCode::kRouteQueueFull;
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

}  // namespace hquant
