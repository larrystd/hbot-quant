#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/types.h"

namespace hquant {

// Fee charged on one of our trades, in the asset it was charged in.
struct TradeFee {
  AssetId asset;
  // Positive means fee paid; negative means rebate received.
  Decimal signed_amount;
};

// 一个账户的一种资产余额。
//   total：持有的全部数量，包括被挂单占用的部分。
//   available：还能用于新订单的数量。实盘由交易所报告；模拟盘由模拟交易所
//              计算。它可能已经扣除了我们的挂单，使用时不要再减一次本地的
//              资金冻结。
struct Balance {
  AccountId account;
  AssetId asset;
  Decimal total;
  Decimal available;
  EventTime time;
};

// 交易所发来的余额变化，字段与 Balance 相同。目前 Shard 忽略它，
// 余额直接从模拟交易所读取。
using BalanceUpdate = Balance;

enum class OrderType { Limit, LimitMaker };
enum class TimeInForce { Gtc, Ioc, Fok };
// 交易所报告的订单状态。Binance 的线上字符串（NEW、FILLED 等）由解析器
// 翻译成这些值。
enum class ExchangeOrderStatus {
  Open,
  PartiallyTraded,
  Traded,
  Canceled,
  Rejected,
  Expired
};
// 给策略和工具看的订单状态。OrderTracker 根据生命周期、确认状态、是否在撤单
// 合成，优先级从高到低：成交明细未到齐（AwaitingTrades）、已结束的状态、
// 下单结果未确认（SubmissionUnknown）、撤单中（PendingCancel）、其余按生命周期。
enum class OrderDisplayState {
  PendingCreate,
  Open,
  PartiallyTraded,
  PendingCancel,
  SubmissionUnknown,
  AwaitingTrades,
  Traded,
  Canceled,
  Failed,
  Expired
};

// 策略想下的单，还没经过任何检查。由策略放在 SubmitOrder 里给出；
// ActionExecutor 先按交易规则把价格和数量取整，再交给风控检查。
//   base_amount：以基础资产计的数量。
//   limit_price：必填，目前只支持限价单。
//   time_in_force：LimitMaker 必须为空（Binance 会拒绝）。
struct OrderRequest {
  AccountId account;
  MarketId market;
  Side side = Side::Buy;
  OrderType type = OrderType::Limit;
  Decimal base_amount;
  std::optional<Decimal> limit_price;
  std::optional<TimeInForce> time_in_force;
};

// 通过风控、已冻结资金的订单，交给 OrderGateway::PrepareSubmit。
//   hold_id：为这张单冻结的资金。
//   action_batch_id：由哪一次策略决策产生。
//   expires_at_mono：到这个时间还没发出就作废（ORDER_EXPIRED_BEFORE_SEND）。
//                    ActionExecutor 设为当前时间 + 1 秒。
struct ApprovedOrder {
  StrategyId strategy_id;
  OrderRequest request;
  HoldId hold_id;
  ActionBatchId action_batch_id;
  MonoTime expires_at_mono{};
};

// 和订单一起保存的策略状态（内容由策略自己定义），用于重启后恢复策略。
// 目前没有策略生成它。
struct StrategyCheckpoint {
  uint32_t schema_version = 0;
  StrategyId strategy_id;
  uint64_t config_revision = 0;
  std::string payload;
};

// 已分配本地订单号、但还没发出的订单。在 StartPrepared 之前先写进订单历史：
// 程序崩溃后，据此知道这张单可能已经到了交易所，去查询它，而不是重发。
// 可能的优化：config_revision 从未被赋值（永远是 0），executor_checkpoint
// 从未被填写；要么删掉，要么等有了策略快照再接上。
struct PreparedOrder {
  ClientOrderId client_id;
  StrategyId strategy_id;
  OrderRequest request;
  uint64_t config_revision = 0;
  UtcTime created_at_utc{};
  std::optional<StrategyCheckpoint> executor_checkpoint;
};

// 交易所（或模拟交易所）发来的"订单 X 的状态变成了 Y"。
// client_id 和 exchange_order_id 至少有一个能确定是哪张订单。
//   cumulative_base / cumulative_quote：到目前为止的累计成交量和成交额（如果
//       交易所提供）。它们可能比逐笔的 TradeUpdate 先到；这时 OrderTracker
//       显示 AwaitingTrades，直到成交明细到齐。
struct OrderUpdate {
  AccountId account;
  MarketId market;
  std::optional<ClientOrderId> client_id;
  std::optional<ExchangeOrderId> exchange_order_id;
  ExchangeOrderStatus exchange_status = ExchangeOrderStatus::Open;
  std::optional<Decimal> cumulative_base;
  std::optional<Decimal> cumulative_quote;
  EventTime time;
};

// "订单 X 刚成交了一笔"：我们自己订单的一笔成交。
//   exchange_trade_id：每笔成交唯一；OrderTracker 据此去掉重复推送。
//   price / base_amount / quote_amount：只是这一笔，不是累计值。
//   maker：我们是否是挂单方（如果交易所提供）。
struct TradeUpdate {
  AccountId account;
  MarketId market;
  std::optional<ClientOrderId> client_id;
  std::optional<ExchangeOrderId> exchange_order_id;
  ExchangeTradeId exchange_trade_id;
  Decimal price;
  Decimal base_amount;
  Decimal quote_amount;
  std::vector<TradeFee> fees;
  std::optional<bool> maker;
  EventTime time;
};

// 一张订单在某一时刻的只读副本，由 OrderTracker 生成。真正的订单
// （order/order_tracker.h 里的 TrackedOrder）只有 OrderTracker 能修改；
// 策略和 Shard 拿到的都是这种副本。
//   exchange_id：交易所确认之前为空（模拟盘始终为空）。
//   request：原始的下单请求。
//   display_state：使用者只需看这一个状态，见 OrderDisplayState。
//   cumulative_base / cumulative_quote：到目前为止的累计成交量和成交额。
//   fees：累计手续费，每种资产一项。
// 可能的优化：每次运行策略前，Shard::OrderViews() 都要为所有提交过的订单
// （包括已结束的）生成快照，开销随运行时间增长；只给未结束的订单（或定期
// 清理已结束的订单）可以避免。另外可以加一个 remaining_base 字段，免得使用者
// 自己计算 request.base_amount - cumulative_base。
struct OrderSnapshot {
  ClientOrderId client_id;
  std::optional<ExchangeOrderId> exchange_id;
  StrategyId strategy_id;
  OrderRequest request;
  OrderDisplayState display_state = OrderDisplayState::PendingCreate;
  Decimal cumulative_base;
  Decimal cumulative_quote;
  std::vector<TradeFee> fees;
  EventTime last_update_time;
};

// 可能的优化：MarketEvent 没有任何地方使用；删掉，或者用作行情交给分片的
// 输入类型。
using MarketEvent = std::variant<BookSnapshot, BookDiff, PublicTrade>;
// Reports about our account from an exchange: order status, fills, balances.
using AccountEvent = std::variant<OrderUpdate, TradeUpdate, BalanceUpdate>;

// OrderTracker 在订单生命周期中产生的事件，每个事件都带着当时的订单快照。
// 可能的优化：目前 OrderTracker 之外没有任何地方使用这些事件，但每处理一条
// 回报都要为它们生成快照。可以改为按需生成，或者只在有使用者时才生成。
struct OrderOpened {
  OrderSnapshot order;
};
struct OrderTraded {
  OrderSnapshot order;
  TradeUpdate trade;
};
struct OrderFullyTraded {
  OrderSnapshot order;
};
struct OrderCanceled {
  OrderSnapshot order;
};
struct OrderFailed {
  OrderSnapshot order;
  std::string reason;
};
using TrackedOrderEvent =
    std::variant<OrderOpened, OrderTraded, OrderFullyTraded, OrderCanceled,
                 OrderFailed>;

// 下单接口。只能由拥有它的分片调用。下单分两步：
//   PrepareSubmit：占用一个有限的发送名额、确定本地订单号、登记为
//                  PendingCreate，但不发送任何数据。调用方随后绑定资金冻结，
//                  并尝试把 PreparedOrder 写进订单历史。
//   StartPrepared：真正发送。返回失败说明一个字节都没发出去；发出后结果不确定
//                  的情况，稍后以账户事件的形式到达。
class OrderGateway {
 public:
  virtual ~OrderGateway() = default;
  virtual absl::StatusOr<PreparedOrder> PrepareSubmit(
      ApprovedOrder approved) = 0;
  virtual absl::Status StartPrepared(const ClientOrderId& client_id) = 0;
  virtual absl::Status AbortPrepared(const ClientOrderId& client_id) = 0;
  virtual absl::Status StartCancel(const StrategyId& strategy_id,
                                   const ClientOrderId& client_id) = 0;
};

// 本地模拟交易所的接口，回放模式和实时行情模拟盘都用它。运行时只依赖这个接口，
// 具体实现（SimpleSimulatedExchange）由上层选择。
//   OnBookBbo / OnPublicTrade：喂入行情价格，用来撮合挂单。
//   DrainEvents：取走积攒的订单回报、成交回报、余额变化。
class SimulatedExchange : public OrderGateway {
 public:
  virtual absl::Status OnBookBbo(const Decimal& bid, const Decimal& ask) = 0;
  virtual absl::Status OnPublicTrade(Side aggressor, const Decimal& price,
                                     const Decimal& public_amount) = 0;
  virtual Decimal BalanceOf(const AssetId& asset) const = 0;
  virtual Decimal AvailableBalance(const AssetId& asset) const = 0;
  virtual std::vector<AccountEvent> DrainEvents() = 0;
};

}  // namespace hquant
