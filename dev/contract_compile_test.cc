#include <type_traits>

#include "application/control_server.h"
#include "base/market.h"
#include "base/net.h"
#include "base/order.h"
#include "base/types.h"
#include "market/order_book.h"
#include "shard/shard.h"
#include "order_history/order_history.h"
#include "strategy/strategy.h"

int main() {
  static_assert(
      !std::is_same_v<hquant::ClientOrderId, hquant::ExchangeOrderId>);
  static_assert(
      !std::is_same_v<hquant::ExchangeOrderId, hquant::ExchangeTradeId>);
  auto price = hquant::Decimal::Parse("100.1");
  auto tick = hquant::Decimal::Parse("0.1");
  if (!price.ok() || !tick.ok()) return 1;
  auto rounded = price->Quantize(*tick, hquant::RoundingMode::Down);
  if (!rounded.ok() || rounded->ToString() != "100.1") return 2;
  if (hquant::Decimal::Parse("NaN").ok()) return 3;
  hquant::MarketId market{hquant::ExchangeId("binance"),
                          hquant::InstrumentKind::Spot, "BTCUSDT"};
  hquant::OrderRequest request;
  request.market = market;
  request.limit_price = *price;
  hquant::ActionBatch batch;
  batch.ordered.emplace_back(
      hquant::SubmitOrder{hquant::StrategyId{}, request});
  hquant::OrderHistoryRecord record;
  record.payload = hquant::PreparedOrder{};
  hquant::ControlRequest server_request;
  server_request.payload = hquant::StatusRequest{};
  hquant::ShardReport report;
  return batch.ordered.size() == 1 && report.report_version == 0 ? 0 : 4;
}
