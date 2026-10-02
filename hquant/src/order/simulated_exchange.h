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

struct SimulatedExchangeConfig {
  AccountId account;
  MarketSpec market;
  TradingRule trading_rule;
  std::map<std::string, Decimal> initial_balances;
  Decimal maker_fee_rate;
  bool buy_fee_from_returns = true;
  std::function<ClientOrderId(Side)> make_client_id;
};

// Deterministic local spot exchange. The caller owns the shard and supplies a
// clock; public trades are only matching triggers, never account fills by
// themselves. All matches fill the entire resting order at its own limit.
class SimpleSimulatedExchange final : public SimulatedExchange {
 public:
  SimpleSimulatedExchange(SimulatedExchangeConfig config, const Clock& clock);

  absl::StatusOr<Order> PrepareSubmit(const SubmitOrder& request,
                                      const StrategyId& strategy_id,
                                      MonoTime expires_at_mono) override;
  absl::Status StartPrepared(const ClientOrderId& client_order_id) override;
  absl::Status AbortPrepared(const ClientOrderId& client_order_id) override;
  absl::Status StartCancel(const ClientOrderId& client_order_id) override;

  absl::Status OnBookBbo(const Decimal& bid, const Decimal& ask) override;
  absl::Status OnPublicTrade(Side aggressor, const Decimal& price,
                             const Decimal& public_amount) override;

  std::vector<Order> OpenOrders() const { return orders_; }
  Decimal BalanceOf(const AssetId& asset) const override;
  Decimal AvailableBalance(const AssetId& asset) const override;
  Decimal FeesPaid(const AssetId& asset) const;
  std::vector<AccountEvent> DrainEvents() override;

 private:
  absl::Status Fill(size_t index);
  absl::Status ValidateAndQuantize(SubmitOrder* request,
                                   const StrategyId& strategy_id) const;
  void EmitOrder(const Order& order, ExchangeOrderStatus status,
                 std::optional<Trade> trade = std::nullopt);
  void EmitBalance(const AssetId& asset);
  EventTime Now() const;

  SimulatedExchangeConfig config_;
  const Clock& clock_;
  std::vector<Order> orders_;
  std::map<std::string, Order> prepared_;
  std::set<std::string> used_ids_;
  std::map<std::string, Decimal> balances_;
  std::map<std::string, Decimal> fees_paid_;
  std::vector<AccountEvent> events_;
  uint64_t next_client_id_ = 1;
  uint64_t next_trade_id_ = 1;
};

}  // namespace hquant
