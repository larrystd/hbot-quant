#pragma once

#include <functional>
#include <vector>

#include "absl/status/status.h"
#include "base/market.h"
#include "base/order.h"
#include "boost/asio/io_context.hpp"
#include "market/order_book.h"

namespace hquant {

// 交易所发来的一条回报（订单状态、成交、余额）交给 Shard 的入口。
using ReportHandler = std::function<void(const AccountEvent&)>;

// 分片里随运行模式（配置的 mode）而不同的部分。Shard 只通过这个接口和交易所
// 打交道，不知道自己是模拟盘还是实盘：
//   模拟盘：SimulatedTrading（simulated_trading.h）
//   实盘：  LiveTrading（live_trading.h）
// 两种模式都把回报交给 SetReportHandler 设置的入口，Shard 用同样的方式处理。
// 所有方法只在分片线程上调用。
class TradingMode {
 public:
  virtual ~TradingMode() = default;

  // 下单、撤单的出口。
  virtual OrderGateway& Gateway() = 0;
  // 回报的入口。Shard 构造时设置一次。
  virtual void SetReportHandler(ReportHandler handler) = 0;
  // 分片线程的 io_context 创建后调用一次。
  virtual void Start(boost::asio::io_context& io) = 0;
  // 订单簿更新之后。
  virtual absl::Status OnBook(const OrderBookView& book) = 0;
  // 市场上有一笔成交之后。
  virtual absl::Status OnPublicTrade(const PublicTrade& trade) = 0;
  // 给策略的余额。
  virtual std::vector<Balance> Balances() const = 0;
};

}  // namespace hquant
