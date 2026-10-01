#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "hbot/base/clock.h"
#include "hbot/base/ids.h"
#include "hbot/connector/api.h"
#include "hbot/model/market.h"
#include "hbot/model/trading_rule.h"
#include "hbot/risk/risk_gate.h"
#include "hbot/storage/ports.h"
#include "hbot/strategy/api.h"

namespace hbot {

struct DispatchContext {
  OwnerId owner;
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
  std::string reason;
  std::optional<ClientOrderId> client_id;
  std::optional<OrderIntent> intent;
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
  bool Record(RecordPayload payload, const OwnerId& owner, UtcTime now);
  void Gap(uint64_t sequence);

  RiskGate& risk_;
  OrderGateway& gateway_;
  RecorderPort& recorder_;
  RunId run_;
  ShardId shard_;
  uint64_t& shard_sequence_;
  std::vector<HistoryGap> local_gaps_;
};

}  // namespace hbot
