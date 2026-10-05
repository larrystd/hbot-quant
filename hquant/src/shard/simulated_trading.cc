#include "shard/simulated_trading.h"

#include <string>
#include <utility>

namespace hquant {

SimulatedTrading::SimulatedTrading(SimulatedExchange& exchange, Config config,
                                   const Clock& clock)
    : exchange_(exchange), config_(std::move(config)), clock_(clock) {}

void SimulatedTrading::SetReportHandler(ReportHandler handler) {
  report_handler_ = std::move(handler);
}

absl::Status SimulatedTrading::OnBook(const OrderBookView& book) {
  if (book.State() != BookSyncState::Live) return absl::OkStatus();
  const auto bid = book.BestBid();
  const auto ask = book.BestAsk();
  if (!bid || !ask) return absl::OkStatus();
  auto bid_price = Price(bid->price_ticks);
  auto ask_price = Price(ask->price_ticks);
  if (!bid_price.ok()) return bid_price.status();
  if (!ask_price.ok()) return ask_price.status();
  auto status = exchange_.OnBookBbo(*bid_price, *ask_price);
  if (!status.ok()) return status;
  DeliverReports();
  return absl::OkStatus();
}

absl::Status SimulatedTrading::OnPublicTrade(const PublicTrade& trade) {
  auto price = Price(trade.price_ticks);
  auto amount = Amount(trade.quantity_lots);
  if (!price.ok()) return price.status();
  if (!amount.ok()) return amount.status();
  auto status = exchange_.OnPublicTrade(*trade.side, *price, *amount);
  if (!status.ok()) return status;
  DeliverReports();
  return absl::OkStatus();
}

std::vector<Balance> SimulatedTrading::Balances() const {
  std::vector<Balance> result;
  for (const AssetId& asset :
       {config_.market.base_asset, config_.market.quote_asset}) {
    result.push_back(Balance{config_.account, asset, exchange_.BalanceOf(asset),
                             exchange_.AvailableBalance(asset),
                             EventTime{{}, clock_.UtcNow(), clock_.MonoNow()}});
  }
  return result;
}

absl::StatusOr<Order> SimulatedTrading::PrepareSubmit(
    const SubmitOrder& request, const StrategyId& strategy_id,
    MonoTime expires_at_mono) {
  auto order = exchange_.PrepareSubmit(request, strategy_id, expires_at_mono);
  DeliverReports();
  return order;
}

absl::Status SimulatedTrading::StartPrepared(
    const ClientOrderId& client_order_id) {
  auto status = exchange_.StartPrepared(client_order_id);
  DeliverReports();
  return status;
}

absl::Status SimulatedTrading::AbortPrepared(
    const ClientOrderId& client_order_id) {
  auto status = exchange_.AbortPrepared(client_order_id);
  DeliverReports();
  return status;
}

absl::Status SimulatedTrading::StartCancel(
    const ClientOrderId& client_order_id) {
  auto status = exchange_.StartCancel(client_order_id);
  DeliverReports();
  return status;
}

void SimulatedTrading::DeliverReports() {
  for (const auto& report : exchange_.DrainEvents())
    if (report_handler_) report_handler_(report);
}

absl::StatusOr<Decimal> SimulatedTrading::Price(PriceTicks ticks) const {
  auto count = Decimal::Parse(std::to_string(ticks.value));
  if (!count.ok()) return count.status();
  return count->Multiply(config_.scale.price_per_tick);
}

absl::StatusOr<Decimal> SimulatedTrading::Amount(QuantityLots lots) const {
  auto count = Decimal::Parse(std::to_string(lots.value));
  if (!count.ok()) return count.status();
  return count->Multiply(config_.scale.amount_per_lot);
}

}  // namespace hquant
