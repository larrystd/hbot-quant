#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <tuple>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hbot/base/ids.h"
#include "hbot/model/market.h"

namespace hbot {

struct OrderOwnership {
  OwnerId owner;
  ShardId current_shard;
};

// The private-stream reader owns this index. Registration and lookup must be
// serialized on that reader; other shards send registrations to it as messages.
// A codec validates the complete venue client ID before returning its stable
// owner key. A shard hint encoded in that ID is never used for routing.
class OrderOwnershipIndex {
 public:
  using DecodeOwnerKey =
      std::function<std::optional<uint64_t>(const ClientOrderId&)>;

  explicit OrderOwnershipIndex(DecodeOwnerKey decode_owner_key = {});

  // Calling again with the same owner/account changes its current shard after
  // a restart. Historical client/exchange mappings keep their logical owner.
  absl::Status SetOwnerRoute(OwnerId owner, AccountId account, ShardId shard);
  absl::Status RegisterClient(ClientOrderId client_id, AccountId account,
                              MarketId market, const OwnerId& owner);
  absl::Status RegisterExchange(
      AccountId account, MarketId market, ExchangeOrderId exchange_id,
      const OwnerId& owner,
      std::optional<ClientOrderId> client_id = std::nullopt);

  absl::StatusOr<OrderOwnership> Resolve(
      const AccountId& account, const MarketId& market,
      const std::optional<ClientOrderId>& client_id,
      const std::optional<ExchangeOrderId>& exchange_id) const;
  bool KnowsAccount(const AccountId& account) const;

 private:
  struct OwnerRoute {
    OwnerId owner;
    AccountId account;
    ShardId shard;
  };
  struct ClientMapping {
    AccountId account;
    MarketId market;
    uint64_t owner_key = 0;
  };
  using ExchangeKey =
      std::tuple<std::string, std::string, int, std::string, std::string>;
  static ExchangeKey Key(const AccountId& account, const MarketId& market,
                         const ExchangeOrderId& exchange_id);

  DecodeOwnerKey decode_owner_key_;
  std::map<uint64_t, OwnerRoute> owners_;
  std::map<std::string, ClientMapping> clients_;
  std::map<ExchangeKey, uint64_t> exchanges_;
};

}  // namespace hbot
