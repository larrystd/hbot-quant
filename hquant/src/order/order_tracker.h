#pragma once

#include <map>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/order.h"

namespace hquant {

// 订单的生命周期。Traded、Canceled、Failed、Expired 是终态：进入终态后，
// 不允许再变成另一个终态（出现这种回报说明数据矛盾，返回错误并标记需要查询）。
enum class OrderLifecycle {
  PendingCreate,
  Open,
  PartiallyTraded,
  Traded,
  Canceled,
  Failed,
  Expired
};
// 我们对订单状态的把握程度。
//   Confirmed：本地状态可信。
//   SubmissionUnknown：请求已发出，但不知道交易所是否收到（超时、5xx）。
//                      资金继续冻结；收到任何该订单的回报后变回 Confirmed。
//   NeedsQuery：回报之间出现矛盾（订单号冲突、成交超量、终态冲突等），
//               本地状态不可信，必须向交易所查询（ApplyQueriedOrder）才能恢复。
enum class ConfirmationState { Confirmed, SubmissionUnknown, NeedsQuery };

// 每次修改订单后返回给调用方的结果。调用方（Shard）据此更新风控、写订单历史。
//   snapshot：修改后的订单快照。
//   events：这次修改产生的生命周期事件（OrderOpened、OrderTraded 等）。
//   changed：这次调用是否真的改变了订单（重复或过期的回报为 false）。
//   needs_order_query：订单是否需要向交易所查询（成交明细未到齐，或者状态
//                      不是 Confirmed）。
struct TrackerResult {
  OrderSnapshot snapshot;
  std::vector<TrackedOrderEvent> events;
  bool changed = false;
  bool cancel_pending = false;
  bool needs_order_query = false;
  ConfirmationState confirmation = ConfirmationState::Confirmed;
};

// 订单状态机：一个分片内所有订单的唯一可信来源，只有它能修改订单状态。
// 单线程、同步执行，只在所属分片的线程上调用；不调用策略，也不调用风控，
// 只返回结果，由 Shard 决定接下来做什么。
//
// 它负责处理现实中的各种乱序和异常：
//   - 成交可能比"已挂上"的确认先到；
//   - "全部成交"的状态可能比最后一笔成交明细先到（显示为 AwaitingTrades）；
//   - 同一笔成交可能推送多次（按成交编号去重）；
//   - 旧的状态回报可能晚到（按生命周期的先后顺序忽略倒退的回报）；
//   - 订单号冲突、成交超过下单量、终态冲突等矛盾（标记 NeedsQuery
//   并返回错误）。
//
// 目前 Shard 只调用 Register、ApplyOrderUpdate、ApplyTradeUpdate、Snapshot。
// RequestCancel、FailBeforeWrite、MarkSubmissionUnknown、ApplyQueriedOrder、
// OrdersNeedingQuery 是为实盘准备的，现在只有测试使用。
//
// 可能的优化：
//   - orders_、exchange_index_、seen_trades_ 只增不删，长时间运行会一直增长；
//     已结束且成交明细到齐的订单可以定期清理（seen_trades_ 需要保留一段时间
//     用于去重）。
//   - 索引的 key 是由多个 std::string 组成的 tuple，每次查找都要构造字符串；
//     可以改成预先计算好的哈希值或整数编号。
//   - events 里的快照目前没有使用者，却每次都要生成（见 base/order.h）。
class OrderTracker {
 public:
  // 登记一张新订单（ActionExecutor 准备好、即将发出的订单），状态为
  // PendingCreate。订单号重复时返回错误。
  absl::StatusOr<TrackerResult> Register(PreparedOrder prepared);
  // 标记"正在撤单"，显示为 PendingCancel，直到收到撤单结果。终态订单不能撤。
  absl::StatusOr<TrackerResult> RequestCancel(
      const ClientOrderId& client_order_id);
  // 发送前就失败了（能证明一个字节都没发出去）：直接把订单置为 Failed。
  // 只有在订单仍是 PendingCreate、没有交易所订单号、没有任何成交时才允许。
  absl::StatusOr<TrackerResult> FailBeforeWrite(
      const ClientOrderId& client_order_id, std::string reason);
  // 发出后结果未知（超时、5xx）：置为 SubmissionUnknown，等回报或查询确认。
  absl::StatusOr<TrackerResult> MarkSubmissionUnknown(
      const ClientOrderId& client_order_id);
  // 处理交易所推来的订单状态变化。
  absl::StatusOr<TrackerResult> ApplyOrderUpdate(const OrderUpdate& update);
  // 处理一笔成交：去重、累计成交量和手续费、检查是否超过下单量。
  absl::StatusOr<TrackerResult> ApplyTradeUpdate(const TradeUpdate& trade);
  // 应用主动查询得到的订单状态。和 ApplyOrderUpdate 的区别：
  // 它可以把 NeedsQuery 恢复为 Confirmed。确认的是原订单号，不会重发订单。
  absl::StatusOr<TrackerResult> ApplyQueriedOrder(const OrderUpdate& update);

  // 某张订单当前的快照；不存在返回空。
  std::optional<OrderSnapshot> Snapshot(
      const ClientOrderId& client_order_id) const;
  // 需要向交易所查询的订单：成交明细未到齐，或者状态不是 Confirmed。
  std::vector<ClientOrderId> OrdersNeedingQuery() const;

 private:
  // 一张订单的完整内部状态，只在 OrderTracker 内部可见。
  //   prepared：登记时的订单（订单号、策略、原始请求）。
  //   exchange_order_id：交易所订单号，第一次在回报里出现时绑定，之后不允许改变。
  //   lifecycle / confirmation / cancel_pending：三者合成对外的
  //   OrderDisplayState。
  //   completion_pending_fills：交易所说已全部成交，但本地累计的成交量还不够，
  //                             说明还有成交明细没到。
  //   created_emitted / terminal_emitted：已经发过"已挂上"、"已结束"事件，
  //                                       防止重复发出。
  //   traded_quantity / traded_value / fees_by_asset：由逐笔成交累计而来。
  struct TrackedOrder {
    PreparedOrder prepared;
    std::optional<ExchangeOrderId> exchange_order_id;
    OrderLifecycle lifecycle = OrderLifecycle::PendingCreate;
    bool cancel_pending = false;
    ConfirmationState confirmation = ConfirmationState::Confirmed;
    bool completion_pending_fills = false;
    bool created_emitted = false;
    bool terminal_emitted = false;
    Decimal traded_quantity;
    Decimal traded_value;
    std::map<std::string, Decimal> fees_by_asset;
    EventTime last_update_time;
  };
  // 索引的 key：（账户，交易所，品种，交易对符号，交易所订单号或成交编号）。
  using ExchangeKey =
      std::tuple<std::string, std::string, int, std::string, std::string>;
  using TradeKey =
      std::tuple<std::string, std::string, int, std::string, std::string>;

  static ExchangeKey ExchangeIndexKey(const AccountId& account,
                                      const MarketId& market,
                                      const ExchangeOrderId& exchange_order_id);
  static TradeKey TradeIndexKey(const AccountId& account,
                                const MarketId& market,
                                const ExchangeTradeId& trade_id);
  static bool Terminal(OrderLifecycle lifecycle);
  OrderDisplayState Display(const TrackedOrder& order) const;
  OrderSnapshot MakeSnapshot(const TrackedOrder& order) const;
  TrackerResult MakeResult(const TrackedOrder& order, bool changed,
                           std::vector<TrackedOrderEvent> events = {}) const;
  // 按本地订单号或交易所订单号找到订单，并检查两者是否一致、账户和交易对
  // 是否匹配；不一致时把相关订单标记为 NeedsQuery 并返回错误。
  absl::StatusOr<TrackedOrder*> Find(
      const AccountId& account, const MarketId& market,
      const std::optional<ClientOrderId>& client_order_id,
      const std::optional<ExchangeOrderId>& exchange_order_id);
  absl::Status BindExchangeId(
      TrackedOrder& order,
      const std::optional<ExchangeOrderId>& exchange_order_id);
  absl::StatusOr<TrackerResult> Update(const OrderUpdate& update,
                                       bool queried_order);

  // 本地订单号 → 订单
  std::map<std::string, TrackedOrder> orders_;
  // 交易所订单号 → 本地订单号
  std::map<ExchangeKey, std::string> exchange_index_;
  // 已处理的成交编号 → 本地订单号，用于去重
  std::map<TradeKey, std::string> seen_trades_;
};

}  // namespace hquant
