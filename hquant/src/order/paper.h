#pragma once

#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/order.h"
#include "base/types.h"

namespace hquant {

struct PaperConfig {
  AccountId account;
  MarketSpec market;
  TradingRule trading_rule;
  std::map<std::string, Decimal> initial_balances;
  Decimal maker_fee_rate;
  bool buy_fee_from_returns = true;
  std::function<ClientOrderId(Side)> make_client_id;
};

struct PaperOrder {
  ClientOrderId client_id;
  OwnerId owner;
  OrderRequest request;
};

// Deterministic local spot exchange. The caller owns the shard and supplies a
// clock; public trades are only matching triggers, never account fills by
// themselves. All matches fill the entire resting order at its own limit.
class PaperConnector final : public SimulatedExchange {
 public:
  PaperConnector(PaperConfig config, const Clock& clock);

  absl::StatusOr<OrderIntent> PrepareSubmit(OrderCommand command) override;
  absl::Status StartPrepared(const ClientOrderId& client_id) override;
  absl::Status AbortPrepared(const ClientOrderId& client_id) override;
  absl::Status StartCancel(const OwnerId& owner,
                           const ClientOrderId& client_id) override;

  absl::Status OnBookBbo(const Decimal& bid, const Decimal& ask) override;
  absl::Status OnPublicTrade(Side aggressor, const Decimal& price,
                             const Decimal& public_amount) override;

  std::vector<PaperOrder> OpenOrders() const { return orders_; }
  Decimal BalanceOf(const AssetId& asset) const override;
  Decimal AvailableBalance(const AssetId& asset) const override;
  Decimal FeesPaid(const AssetId& asset) const;
  std::vector<AccountEvent> DrainEvents() override;

 private:
  absl::Status Fill(size_t index);
  absl::Status ValidateAndQuantize(OrderCommand* command) const;
  void EmitOrder(const PaperOrder& order, ExchangeOrderStatus status);
  void EmitBalance(const AssetId& asset);
  EventTime Now() const;

  PaperConfig config_;
  const Clock& clock_;
  std::vector<PaperOrder> orders_;
  std::map<std::string, OrderCommand> prepared_;
  std::set<std::string> used_ids_;
  std::map<std::string, Decimal> balances_;
  std::map<std::string, Decimal> fees_paid_;
  std::vector<AccountEvent> events_;
  uint64_t next_client_id_ = 1;
  uint64_t next_trade_id_ = 1;
};

}  // namespace hquant
