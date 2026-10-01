#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "base/error.h"
#include "base/market.h"
#include "base/order.h"
#include "base/types.h"
#include "offline/storage.h"
#include "order/risk.h"
#include "strategy/strategy.h"

namespace hquant {

struct DispatchContext {
  StrategyId strategy_id;
  DecisionId decision_id;
  MarketSpec market;
  TradingRule rule;
  UtcTime now_utc{};
  MonoTime now_mono{};
  bool market_live = false;
  bool account_fresh = false;
};

struct DispatchResult {
  uint32_t action_index = 0;
  bool accepted = false;
  ErrorCode reason = ErrorCode::kOk;
  std::string message;
  std::optional<ClientOrderId> client_id;
  std::optional<PreparedOrder> prepared;
  std::optional<ReservationId> reservation_id;
};

// Single-shard synchronous dispatcher. Only the owning shard calls Dispatch.
class ActionDispatcher {
 public:
  ActionDispatcher(RiskGate& risk, OrderGateway& gateway,
                   RecorderPort& recorder, RunId run, ShardId shard,
                   uint64_t& shard_sequence)
      : risk_(risk),
        gateway_(gateway),
        recorder_(recorder),
        run_(run),
        shard_(shard),
        shard_sequence_(shard_sequence) {}

  std::vector<DispatchResult> Dispatch(const ActionBatch& batch,
                                       const DispatchContext& context);
  const std::vector<HistoryGap>& local_gaps() const { return local_gaps_; }

 private:
  bool Record(RecordPayload payload, const StrategyId& strategy_id,
              UtcTime now);
  void Gap(uint64_t sequence);

  RiskGate& risk_;
  OrderGateway& gateway_;
  RecorderPort& recorder_;
  RunId run_;
  ShardId shard_;
  uint64_t& shard_sequence_;
  std::vector<HistoryGap> local_gaps_;
};

}  // namespace hquant
