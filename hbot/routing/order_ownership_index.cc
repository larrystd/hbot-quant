#include "hbot/routing/order_ownership_index.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "absl/status/status.h"

namespace hbot {

OrderOwnershipIndex::OrderOwnershipIndex(DecodeOwnerKey decode_owner_key)
    : decode_owner_key_(std::move(decode_owner_key)) {}

OrderOwnershipIndex::ExchangeKey OrderOwnershipIndex::Key(
    const AccountId& account, const MarketId& market,
    const ExchangeOrderId& exchange_id) {
  return {account.value, market.venue.value,
          static_cast<int>(market.instrument_kind), market.native_symbol,
          exchange_id.value};
}

absl::Status OrderOwnershipIndex::SetOwnerRoute(OwnerId owner,
                                                AccountId account,
                                                ShardId shard) {
  if (!owner.IsValid() || account.value.empty() || !shard.IsValid()) {
    return absl::InvalidArgumentError("invalid owner route");
  }
  auto found = owners_.find(owner.owner_key);
  if (found != owners_.end()) {
    if (found->second.owner != owner || found->second.account != account) {
      return absl::FailedPreconditionError("owner key identity conflict");
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
      market.venue.value.empty() || market.native_symbol.empty()) {
    return absl::InvalidArgumentError("invalid client ownership key");
  }
  const auto route = owners_.find(owner.owner_key);
  if (route == owners_.end() || route->second.owner != owner ||
      route->second.account != account) {
    return absl::FailedPreconditionError("client owner has no matching route");
  }
  if (decode_owner_key_) {
    auto decoded = decode_owner_key_(client_id);
    if (decoded && *decoded != owner.owner_key) {
      return absl::FailedPreconditionError(
          "client ID owner conflicts with registration");
    }
  }
  auto [it, inserted] = clients_.emplace(
      client_id.value, ClientMapping{account, market, owner.owner_key});
  if (!inserted &&
      (it->second.account != account || it->second.market != market ||
       it->second.owner_key != owner.owner_key)) {
    return absl::FailedPreconditionError("client ID ownership conflict");
  }
  return absl::OkStatus();
}

absl::Status OrderOwnershipIndex::RegisterExchange(
    AccountId account, MarketId market, ExchangeOrderId exchange_id,
    const OwnerId& owner, std::optional<ClientOrderId> client_id) {
  if (exchange_id.value.empty() || account.value.empty() ||
      market.venue.value.empty() || market.native_symbol.empty()) {
    return absl::InvalidArgumentError("invalid exchange ownership key");
  }
  const auto route = owners_.find(owner.owner_key);
  if (route == owners_.end() || route->second.owner != owner ||
      route->second.account != account) {
    return absl::FailedPreconditionError(
        "exchange owner has no matching route");
  }
  if (client_id) {
    auto resolved = Resolve(account, market, client_id, std::nullopt);
    if (!resolved.ok() || resolved->owner != owner) {
      return absl::FailedPreconditionError(
          "exchange/client ownership conflict");
    }
  }
  auto [it, inserted] =
      exchanges_.emplace(Key(account, market, exchange_id), owner.owner_key);
  if (!inserted && it->second != owner.owner_key) {
    return absl::FailedPreconditionError("exchange ID ownership conflict");
  }
  return absl::OkStatus();
}

absl::StatusOr<OrderOwnership> OrderOwnershipIndex::Resolve(
    const AccountId& account, const MarketId& market,
    const std::optional<ClientOrderId>& client_id,
    const std::optional<ExchangeOrderId>& exchange_id) const {
  if (account.value.empty() || market.venue.value.empty() ||
      market.native_symbol.empty() || (client_id && client_id->value.empty()) ||
      (exchange_id && exchange_id->value.empty())) {
    return absl::InvalidArgumentError("invalid private report identity");
  }
  std::optional<uint64_t> candidate;
  const auto merge = [&](uint64_t owner_key) -> absl::Status {
    if (candidate && *candidate != owner_key) {
      return absl::FailedPreconditionError("private report ownership conflict");
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
        return absl::FailedPreconditionError(
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
  if (!candidate) return absl::NotFoundError("private report owner unknown");
  const auto route = owners_.find(*candidate);
  if (route == owners_.end()) return absl::NotFoundError("owner route unknown");
  if (route->second.account != account) {
    return absl::FailedPreconditionError("owner/account conflict");
  }
  return OrderOwnership{route->second.owner, route->second.shard};
}

bool OrderOwnershipIndex::KnowsAccount(const AccountId& account) const {
  for (const auto& [key, route] : owners_) {
    if (route.account == account) return true;
  }
  return false;
}

}  // namespace hbot
