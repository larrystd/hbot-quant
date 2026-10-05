#pragma once

#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "base/market.h"
#include "base/order.h"
#include "shard/trading_mode.h"

namespace hquant {

// 实盘：订单由网关发给交易所，成交由交易所决定，回报从网络到达后交给
// 回报入口。
// 尚未实现：
//   Start：启动账户推送，在分片线程上接收订单回报、成交、余额变化；
//   网关事件（发送前失败、结果未知、交易所确认或拒绝）转成回报；
//   Balances：返回账户推送更新的余额。
class LiveTrading final : public TradingMode {
 public:
  explicit LiveTrading(OrderGateway& gateway) : gateway_(gateway) {}

  OrderGateway& Gateway() override { return gateway_; }
  void SetReportHandler(ReportHandler handler) override {
    report_handler_ = std::move(handler);
  }
  void Start(boost::asio::io_context&) override {}
  // 撮合在交易所完成，行情之后不需要做任何事。
  absl::Status OnBook(const OrderBookView&) override {
    return absl::OkStatus();
  }
  absl::Status OnPublicTrade(const PublicTrade&) override {
    return absl::OkStatus();
  }
  std::vector<Balance> Balances() const override { return {}; }

 private:
  OrderGateway& gateway_;
  ReportHandler report_handler_;
};

}  // namespace hquant
