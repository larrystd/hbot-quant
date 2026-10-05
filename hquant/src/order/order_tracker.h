#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/order.h"

namespace hquant {

// Only the tracker interprets exchange reports as execution state. Its Order
// values remain stable for the life of the tracker. Returned pointers become
// invalid when the tracker is destroyed.
class OrderTracker {
 public:
  absl::Status Add(const Order& order);
  absl::StatusOr<UpdateResult> OnSendResult(const ClientOrderId& id,
                                            SendResult result);
  absl::StatusOr<UpdateResult> OnCancelRequested(const ClientOrderId& id);
  // 把交易所发来的一条订单消息应用到对应订单上：带成交时先去重、累加成交量，
  // 再记录交易所说的累计量和状态，最后推导订单的结论状态。
  //   返回 Ignored：订单没变（重复或过期的消息）；
  //        Updated：订单变了但没结束；
  //        Finished：订单这次结束，且成交明细已到齐。
  //   source：只有 Query（主动查询的结果）能让 NeedsQuery 的订单恢复，也只有
  //           它能让状态倒退；Push 的旧消息会被忽略。
  // 注意：返回错误时，订单可能已被标记为 NeedsQuery，错误不代表没有修改。
  absl::StatusOr<UpdateResult> Apply(const OrderUpdate& update,
                                     ReportSource source);

  const Order* Find(const ClientOrderId& id) const;
  bool HasTrade(const ExchangeTradeId& id) const;
  std::vector<const Order*> ActiveOrders() const;
  std::vector<ClientOrderId> OrdersToQuery() const;

 private:
  // 订单跟踪器根据交易所消息推导出的结论。交易所只知道自己的状态；
  // PendingCancel、SubmissionUnknown、NeedsQuery、AwaitingTrades 是只有我们
  // 这边才知道的情况。
  enum class Status {
    PendingCreate,      // 已登记，交易所还没确认
    Open,               // 挂单中
    PartiallyTraded,    // 部分成交，仍在挂
    PendingCancel,      // 已请求撤单，还没确认
    SubmissionUnknown,  // 已发出，不知道交易所是否收到；资金继续冻结，需要查询
    NeedsQuery,         // 消息之间矛盾，本地状态不可信；只有查询结果能恢复
    AwaitingTrades,     // 交易所说已结束，但成交明细没到齐；资金暂不释放
    Traded,             // 以下为结束状态
    Canceled,
    Failed,
    Expired
  };
  // 一张订单的全部内部状态。前三个"收到了什么"，status 是"据此判断出什么"：
  //   executed_quantity：我们自己把收到的成交逐笔累加的数量。
  //   reported_quantity：交易所说的累计成交量（最后一次可信的值）。
  //   exchange_status：交易所说的最新状态。
  // 交易所的消息可能乱序（状态说已全部成交，成交明细却没到齐），所以
  // "交易所说的"和"我们累加的"分开记，比较两者得出 status。
  struct Entry {
    Order order;
    Status status = Status::PendingCreate;
    Decimal executed_quantity;
    std::optional<Decimal> reported_quantity;
    std::optional<ExchangeOrderStatus> exchange_status;
  };

  static bool Finished(Status status);
  static bool Active(Status status);
  static Status TerminalStatus(ExchangeOrderStatus status);
  // 本地订单号 → 订单。
  std::map<std::string, Entry> orders_;
  // 已处理的成交编号 → 所属订单的本地订单号。用于：
  //   - 去重：同一笔成交重复推送时不再累加；
  //   - 发现矛盾：同一个成交编号出现在另一张单上。
  // 目前只增不删，已结束订单的编号保留多久还没有定。
  std::map<std::string, std::string> seen_trades_;
};

}  // namespace hquant
