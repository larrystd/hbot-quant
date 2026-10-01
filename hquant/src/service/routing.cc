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

OrderStrategyIndex::OrderStrategyIndex(DecodeStrategyId decode_strategy_id)
    : decode_strategy_id_(std::move(decode_strategy_id)) {}

OrderStrategyIndex::ExchangeKey OrderStrategyIndex::Key(
    const AccountId& account, const MarketId& market,
    const ExchangeOrderId& exchange_id) {
  return {account.value, market.exchange.value,
          static_cast<int>(market.instrument_kind), market.native_symbol,
          exchange_id.value};
}

absl::Status OrderStrategyIndex::SetStrategyRoute(StrategyId strategy_id,
                                                AccountId account,
                                                ShardId shard) {
  if (!strategy_id.IsValid() || account.value.empty() || !shard.IsValid()) {
    return Error(ErrorCode::kRouteConfigInvalid, "invalid strategy_id route");
  }
  auto found = strategy_routes_.find(strategy_id.value);
  if (found != strategy_routes_.end()) {
    if (found->second.strategy_id != strategy_id || found->second.account != account) {
      return Error(ErrorCode::kRouteConfigInvalid,
                   "strategy_id key identity conflict");
    }
    found->second.shard = shard;
  } else {
    strategy_routes_.emplace(strategy_id.value,
                    StrategyRoute{std::move(strategy_id), std::move(account), shard});
  }
  return absl::OkStatus();
}

absl::Status OrderStrategyIndex::RegisterClient(ClientOrderId client_id,
                                                 AccountId account,
                                                 MarketId market,
                                                 const StrategyId& strategy_id) {
  if (client_id.value.empty() || account.value.empty() ||
      market.exchange.value.empty() || market.native_symbol.empty()) {
    return Error(ErrorCode::kRouteReportInvalid,
                 "invalid client strategy key");
  }
  const auto route = strategy_routes_.find(strategy_id.value);
  if (route == strategy_routes_.end() || route->second.strategy_id != strategy_id ||
      route->second.account != account) {
    return Error(ErrorCode::kRouteOwnerUnknown,
                 "client strategy_id has no matching route");
  }
  if (decode_strategy_id_) {
    auto decoded = decode_strategy_id_(client_id);
    if (decoded && *decoded != strategy_id.value) {
      return Error(ErrorCode::kRouteOwnershipConflict,
                   "client ID strategy_id conflicts with registration");
    }
  }
  auto [it, inserted] = clients_.emplace(
      client_id.value, ClientMapping{account, market, strategy_id.value});
  if (!inserted &&
      (it->second.account != account || it->second.market != market ||
       it->second.strategy_id != strategy_id.value)) {
    return Error(ErrorCode::kRouteOwnershipConflict,
                 "client ID strategy conflict");
  }
  return absl::OkStatus();
}

absl::Status OrderStrategyIndex::RegisterExchange(
    AccountId account, MarketId market, ExchangeOrderId exchange_id,
    const StrategyId& strategy_id, std::optional<ClientOrderId> client_id) {
  if (exchange_id.value.empty() || account.value.empty() ||
      market.exchange.value.empty() || market.native_symbol.empty()) {
    return Error(ErrorCode::kRouteReportInvalid,
                 "invalid exchange strategy key");
  }
  const auto route = strategy_routes_.find(strategy_id.value);
  if (route == strategy_routes_.end() || route->second.strategy_id != strategy_id ||
      route->second.account != account) {
    return Error(ErrorCode::kRouteOwnerUnknown,
                 "exchange strategy_id has no matching route");
  }
  if (client_id) {
    auto resolved = Resolve(account, market, client_id, std::nullopt);
    if (!resolved.ok() || resolved->strategy_id != strategy_id) {
      return Error(ErrorCode::kRouteOwnershipConflict,
                   "exchange/client strategy conflict");
    }
  }
  auto [it, inserted] =
      exchanges_.emplace(Key(account, market, exchange_id), strategy_id.value);
  if (!inserted && it->second != strategy_id.value) {
    return Error(ErrorCode::kRouteOwnershipConflict,
                 "exchange ID strategy conflict");
  }
  return absl::OkStatus();
}

absl::StatusOr<OrderRoute> OrderStrategyIndex::Resolve(
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
  const auto merge = [&](uint64_t strategy_id) -> absl::Status {
    if (candidate && *candidate != strategy_id) {
      return Error(ErrorCode::kRouteOwnershipConflict,
                   "private report strategy conflict");
    }
    candidate = strategy_id;
    return absl::OkStatus();
  };
  if (client_id && decode_strategy_id_) {
    auto decoded = decode_strategy_id_(*client_id);
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
      auto status = merge(found->second.strategy_id);
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
    return Error(ErrorCode::kRouteOwnerUnknown, "private report strategy_id unknown");
  const auto route = strategy_routes_.find(*candidate);
  if (route == strategy_routes_.end())
    return Error(ErrorCode::kRouteOwnerUnknown, "strategy_id route unknown");
  if (route->second.account != account) {
    return Error(ErrorCode::kRouteOwnershipConflict, "strategy_id/account conflict");
  }
  return OrderRoute{route->second.strategy_id, route->second.shard};
}

bool OrderStrategyIndex::KnowsAccount(const AccountId& account) const {
  for (const auto& [key, route] : strategy_routes_) {
    if (route.account == account) return true;
  }
  return false;
}

}  // namespace hquant

namespace hquant {

PrivateReportRouter::PrivateReportRouter(Options options,
                                         OrderStrategyIndex& index)
    : options_(options), index_(index) {
  for (size_t shard = 0; shard < queues_.size(); ++shard) {
    if (shard != options_.reader_shard.value) {
      queues_[shard] =
          std::make_unique<Queue>(options_.queue_capacity_per_shard);
    }
  }
}

absl::StatusOr<std::unique_ptr<PrivateReportRouter>>
PrivateReportRouter::Create(Options options, OrderStrategyIndex& index) {
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
  auto route = index_.Resolve(account, market, client_id, exchange_id);
  if (!route.ok()) {
    result.failure = CodeOf(route.status());
    return Quarantine(std::move(report), std::move(result));
  }
  RoutedPrivateReport routed{route->strategy_id, route->current_shard,
                             result.source_sequence, std::move(report)};
  if (route->current_shard == options_.reader_shard) {
    result.disposition = RouteDisposition::Local;
    result.local_report = std::move(routed);
    return result;
  }
  auto& queue = *queues_[route->current_shard.value];
  const size_t tail = queue.tail.load(std::memory_order_relaxed);
  const size_t head = queue.head.load(std::memory_order_acquire);
  if (tail - head >= queue.slots.size()) {
    result.failure = ErrorCode::kRouteQueueFull;
    return Quarantine(std::move(routed.report), std::move(result));
  }
  queue.slots[tail % queue.slots.size()] = std::move(routed);
  queue.tail.store(tail + 1, std::memory_order_release);
  result.disposition = RouteDisposition::Forwarded;
  if (tail == head) result.wake_shard = route->current_shard;
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
