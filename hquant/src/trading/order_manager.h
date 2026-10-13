#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hquant/trading/order_state.h"
#include "hquant/trading/trade_port.h"

namespace hquant::v1 {

// 订单索引与查单计划；状态迁移交给 OrderState。
// 每个实例只由其所属 worker 调用，本类不发请求、不写数据库。
class OrderManager {
 public:
  using Clock = std::chrono::steady_clock;

  absl::StatusOr<OrderId> NextLocalOrderId();
  absl::Status Begin(TrackedOrder order);
  absl::Status AbortSubmit(const ClientOrderId& id);
  absl::Status BeginCancel(const ClientOrderId& id,
                           uint64_t source_bench_seq = 0,
                           uint64_t requested_ms = 0);
  absl::Status AbortCancel(const ClientOrderId& id);
  absl::Status MarkSubmitUnknown(const ClientOrderId& id);
  absl::Status MarkCancelUnknown(const ClientOrderId& id);
  absl::Status MarkCancelRejected(const ClientOrderId& id);
  absl::Status MarkWriting(const ClientOrderId& id, bool cancel);
  absl::StatusOr<bool> ApplyReport(const OrderReport& report);
  absl::StatusOr<bool> ApplyFill(const Fill& fill);

  const TrackedOrder* Find(const ClientOrderId& id) const {
    return state_.Find(id);
  }
  const TrackedOrder* FindLocal(OrderId id) const;
  const std::map<std::string, TrackedOrder>& Orders() const {
    return state_.Orders();
  }
  const OrderState& State() const { return state_; }
  // OpenFor 返回未终结订单；PendingFor 检查提交／撤单是否仍待确认。
  std::vector<OrderId> OpenFor(ShardId shard) const;
  bool PendingFor(ShardId shard) const;

  // 未决订单的定时查单计划；实际请求由 TradingAccount 发送。
  void Arm(const ClientOrderId& id, Clock::time_point now = Clock::now());
  void QueryFinished(const ClientOrderId& id);
  void RearmPending(Clock::time_point now = Clock::now());
  std::optional<Clock::time_point> NextDeadline() const;
  std::optional<QueryCommand> TakeDue(Clock::time_point now = Clock::now());
  bool RepeatedQuery(const ClientOrderId& id) const;

 private:
  struct QueryPlan {
    // 下次允许发起查询的时间。
    Clock::time_point due;
    // 已发起次数，查询失败也计入。
    uint8_t attempts = 0;
    // 防止同一订单并发查询。
    bool in_flight = false;
  };
  void RefreshPlan(const ClientOrderId& id);
  void ErasePlan(const ClientOrderId& id);

  // 本地订单编号在恢复 SubmitIntent 时推进。
  uint64_t next_local_order_id_ = 1;
  // 每笔订单的权威状态。
  OrderState state_;
  // 本地订单 ID → 客户订单 ID；终结后仍保留。
  std::map<uint64_t, ClientOrderId> local_ids_;
  // Shard ID → 未终结订单的客户订单 ID。
  std::map<uint16_t, std::set<std::string>> active_order_ids_;
  // 客户订单 ID → 未决订单的查单计划。
  std::map<std::string, QueryPlan> queries_;
  // Non-in-flight queries ordered by due time.
  std::set<std::pair<Clock::time_point, std::string>> due_queries_;
};

}  // namespace hquant::v1
