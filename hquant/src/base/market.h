#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/types.h"

namespace hquant {

enum class InstrumentKind { Spot };

// 一个可交易的交易对：哪个交易所、什么品种、交易所自己的符号。
// 三个字段全部相同才算同一个交易对。
//   native_symbol：保留交易所原样的写法（Binance 是 "BTCUSDT"，有的交易所是
//                  "BTC-USDT"），发请求、解析消息时直接使用，不做转换。
//                  不从它推断资产，资产见 MarketSpec。
struct MarketId {
  ExchangeId exchange;
  InstrumentKind instrument_kind = InstrumentKind::Spot;
  std::string native_symbol;
  bool operator==(const MarketId&) const = default;

  template <typename H>
  friend H AbslHashValue(H hash, const MarketId& market) {
    return H::combine(std::move(hash), market.exchange, market.instrument_kind,
                      market.native_symbol);
  }
};

// 交易对及其两种资产，都来自配置。
//   base_asset：基础资产，买卖的对象（BTC-USDT 里的 BTC）。
//   quote_asset：计价资产，用来报价和付款（BTC-USDT 里的 USDT）。
struct MarketSpec {
  MarketId market;
  AssetId base_asset;
  AssetId quote_asset;
};

// 用整数表示的价格：价格 = value × price_per_tick。
// 订单簿内部只用整数，比较和定位又准又快；只在订单簿的进出口才换算回 Decimal。
struct PriceTicks {
  int64_t value = 0;
  bool operator==(const PriceTicks&) const = default;
};
// 用整数表示的数量：数量 = value × amount_per_lot。
struct QuantityLots {
  uint64_t value = 0;
  bool operator==(const QuantityLots&) const = default;
};

// 订单簿整数与真实价格、数量之间的换算系数，来自配置。
//   price_per_tick / amount_per_lot：一个价格刻度、一个数量刻度的大小。
//       交易所的价格或数量不能被整除时拒绝（FEED_TICK_SIZE_MISMATCH）。
//   tick_lot_version：系数每变一次加 1。快照和增量都带着它，
//       用旧系数换算出来的数据不会被应用。
// 可能的优化：Shard::Price/Amount 每次买一卖一变化都要经过 std::to_string
// 和 Decimal::Parse
// 换算；把系数缓存成整数的倍数或小数位数，可以做到不分配内存。
struct TickLotSize {
  Decimal price_per_tick;
  Decimal amount_per_lot;
  uint64_t tick_lot_version = 0;
  bool IsValid() const {
    return price_per_tick.IsStrictlyPositive() &&
           amount_per_lot.IsStrictlyPositive() && tick_lot_version > 0;
  }
};

enum class Side { Buy, Sell };

// 订单簿的一档。在增量里，quantity_lots 是该价位变化后的数量（绝对值，
// 不是加减量），为 0 表示删除这一档。
struct BookLevel {
  PriceTicks price_ticks;
  QuantityLots quantity_lots;
};

// 截至某个序号的完整订单簿（Binance REST 深度快照）。
//   connection_id：哪次连接取到的；订单簿忽略旧连接的数据。
//   last_sequence：订单簿包含截至这个序号的全部更新（Binance 的
//   lastUpdateId）。
//                  之后应用的第一条增量，起始序号必须 ≤ last_sequence + 1。
//   bids / asks：各档价格；快照里数量不会为 0。
//   time：接收时间，也用来判断订单簿是否过期。
struct BookSnapshot {
  MarketId market;
  uint64_t tick_lot_version = 0;
  uint64_t connection_id = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
  EventTime time;
};

// 第 first_sequence 到 last_sequence 号更新的变化（Binance 的 U..u）。
// 只有接得上订单簿才应用：last_sequence ≤ 订单簿当前序号，是旧消息，丢弃；
// first_sequence > 订单簿当前序号 + 1，是缺口，必须重新同步。
// 和已应用的部分重叠没有关系，因为数量是绝对值。
// 可能的优化：解析器每条增量都分配两个 vector；等快照期间还要复制一份进
// OrderBookSync 的缓存。实时路径可以复用解析器的缓冲区，或者不经过这个中间
// 对象，直接把各档应用到订单簿。
struct BookDiff {
  MarketId market;
  uint64_t tick_lot_version = 0;
  uint64_t connection_id = 0;
  uint64_t first_sequence = 0;
  uint64_t last_sequence = 0;
  std::vector<BookLevel> bids;
  std::vector<BookLevel> asks;
  EventTime time;
};

// 市场上别人之间的成交，属于行情。它永远不算作我们自己订单的成交；
// 模拟交易所只把它当作撮合的触发条件。
//   side：主动成交（吃单）的一方；Shard 拒绝没有方向的成交。
struct PublicTrade {
  MarketId market;
  std::optional<std::string> public_trade_id;
  PriceTicks price_ticks;
  QuantityLots quantity_lots;
  std::optional<Side> side;
  EventTime time;
};

// 一个交易对的下单规则，订单发出前检查。
//   price_increment / base_increment：价格、数量向下取整到这两个步长；
//       没对齐步长的订单被拒绝（ORDER_NOT_ON_TICK）。
//   min_base_amount / max_base_amount / min_order_value：数量和金额的限制，
//       金额 = 数量 × 价格。
//   revision / observed_at：规则是什么时候得到的；规则超过 RiskGate 的
//       max_rule_age 后，拒绝下单（RISK_TRADING_RULE_STALE）。
// 可能的优化：目前规则只来自配置。长时间实盘运行时，定期从交易所
// （exchangeInfo）刷新，可以让 observed_at 保持最新。
struct TradingRule {
  MarketId market;
  Decimal price_increment;
  Decimal base_increment;
  Decimal min_base_amount;
  Decimal min_order_value;
  std::optional<Decimal> max_base_amount;
  uint64_t revision = 0;
  UtcTime observed_at{};
};

}  // namespace hquant
