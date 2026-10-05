#pragma once

#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/market.h"
#include "base/order.h"
#include "base/types.h"
#include "shard/trading_mode.h"

namespace hquant {

// 模拟盘：订单不发到交易所，交给模拟交易所。模拟交易所根据实时行情判断挂单
// 是否成交。它的回报是同步产生的：每次下单、撤单、撮合之后，立刻取走，
// 交给回报入口。
//
// 它自己就是下单出口（OrderGateway）：把调用转给模拟交易所，再把产生的
// 回报交出去。
class SimulatedTrading final : public TradingMode, public OrderGateway {
 public:
  struct Config {
    AccountId account;
    MarketSpec market;
    TickLotSize scale;  // 订单簿刻度和价格、数量之间的换算
  };

  SimulatedTrading(SimulatedExchange& exchange, Config config,
                   const Clock& clock);

  // TradingMode
  OrderGateway& Gateway() override { return *this; }
  void SetReportHandler(ReportHandler handler) override;
  void Start(boost::asio::io_context&) override {}
  // 用最新的买一卖一撮合挂单。
  absl::Status OnBook(const OrderBookView& book) override;
  // 用这笔市场成交撮合挂单。
  absl::Status OnPublicTrade(const PublicTrade& trade) override;
  // 直接读模拟交易所的余额。
  std::vector<Balance> Balances() const override;

  // OrderGateway：转给模拟交易所，然后交出回报。
  absl::StatusOr<Order> PrepareSubmit(const SubmitOrder& request,
                                      const StrategyId& strategy_id,
                                      MonoTime expires_at_mono) override;
  absl::Status StartPrepared(const ClientOrderId& client_order_id) override;
  absl::Status AbortPrepared(const ClientOrderId& client_order_id) override;
  absl::Status StartCancel(const ClientOrderId& client_order_id) override;

 private:
  // 取走模拟交易所积攒的回报，逐条交给回报入口。
  void DeliverReports();
  absl::StatusOr<Decimal> Price(PriceTicks ticks) const;
  absl::StatusOr<Decimal> Amount(QuantityLots lots) const;

  SimulatedExchange& exchange_;
  Config config_;
  const Clock& clock_;
  ReportHandler report_handler_;
};

}  // namespace hquant
