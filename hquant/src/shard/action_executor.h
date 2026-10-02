#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "base/error.h"
#include "base/market.h"
#include "base/order.h"
#include "base/types.h"
#include "order/risk.h"
#include "order_history/order_history.h"
#include "strategy/strategy.h"

namespace hquant {

struct ActionContext {
  StrategyId strategy_id;
  ActionBatchId action_batch_id;
  MarketSpec market;
  TradingRule rule;
  UtcTime now_utc{};
  MonoTime now_mono{};
  bool market_live = false;
  bool account_fresh = false;
};

struct ActionResult {
  uint32_t action_index = 0;
  bool accepted = false;
  ErrorCode reason = ErrorCode::kOk;
  std::string message;
  std::optional<ClientOrderId> client_order_id;
  std::optional<PreparedOrder> prepared;
  std::optional<HoldId> hold_id;
};

// Executes one shard's strategy actions synchronously on the owning thread.
class ActionExecutor {
 public:
  ActionExecutor(RiskGate& risk, OrderGateway& gateway, OrderHistoryWriter& recorder,
                 RunId run, ShardId shard, uint64_t& shard_sequence)
      : risk_(risk),
        gateway_(gateway),
        recorder_(recorder),
        run_(run),
        shard_(shard),
        shard_sequence_(shard_sequence) {}

  std::vector<ActionResult> Execute(const ActionBatch& batch,
                                    const ActionContext& context);
  const std::vector<OrderHistoryGap>& local_gaps() const { return local_gaps_; }

 private:
  bool Record(OrderHistoryRecordPayload payload, const StrategyId& strategy_id,
              UtcTime now);
  void Gap(uint64_t sequence);

  RiskGate& risk_;
  OrderGateway& gateway_;
  OrderHistoryWriter& recorder_;
  RunId run_;
  ShardId shard_;
  uint64_t& shard_sequence_;
  std::vector<OrderHistoryGap> local_gaps_;
};

}  // namespace hquant
