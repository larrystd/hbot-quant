#include "order/simulated_exchange.h"

#include <algorithm>
#include <string>
#include <utility>

#include "base/error.h"

namespace hquant {
namespace {

Decimal Zero() { return *Decimal::Parse("0"); }

bool AtLeast(const Decimal& left, const Decimal& right) {
  auto result = left.Compare(right);
  return result.ok() && *result >= 0;
}

}  // namespace

SimpleSimulatedExchange::SimpleSimulatedExchange(SimulatedExchangeConfig config,
                                                 const Clock& clock)
    : config_(std::move(config)),
      clock_(clock),
      balances_(config_.initial_balances) {}

EventTime SimpleSimulatedExchange::Now() const {
  return EventTime{{}, clock_.UtcNow(), clock_.MonoNow()};
}

Decimal SimpleSimulatedExchange::BalanceOf(const AssetId& asset) const {
  auto it = balances_.find(asset.value);
  return it == balances_.end() ? Zero() : it->second;
}

Decimal SimpleSimulatedExchange::AvailableBalance(const AssetId& asset) const {
  Decimal result = BalanceOf(asset);
  for (const auto& order : orders_) {
    if (order.request.side == Side::Buy &&
        asset == config_.market.quote_asset) {
      auto hold = order.request.quantity.Multiply(*order.request.limit_price);
      if (hold.ok() && !config_.buy_fee_from_returns) {
        auto fee = hold->Multiply(config_.maker_fee_rate);
        if (fee.ok()) hold = hold->Add(*fee);
      }
      if (hold.ok()) {
        auto remaining = result.Subtract(*hold);
        if (remaining.ok()) result = *remaining;
      }
    } else if (order.request.side == Side::Sell &&
               asset == config_.market.base_asset) {
      auto remaining = result.Subtract(order.request.quantity);
      if (remaining.ok()) result = *remaining;
    }
  }
  return result;
}

Decimal SimpleSimulatedExchange::FeesPaid(const AssetId& asset) const {
  auto it = fees_paid_.find(asset.value);
  return it == fees_paid_.end() ? Zero() : it->second;
}

std::vector<AccountEvent> SimpleSimulatedExchange::DrainEvents() {
  std::vector<AccountEvent> result;
  result.swap(events_);
  return result;
}

void SimpleSimulatedExchange::EmitOrder(const RestingOrder& order,
                                        ExchangeOrderStatus status) {
  OrderUpdate update;
  update.account = config_.account;
  update.market = config_.market.market;
  update.client_order_id = order.client_order_id;
  update.exchange_status = status;
  update.time = Now();
  if (status == ExchangeOrderStatus::Traded) {
    update.traded_quantity = order.request.quantity;
    auto quote = order.request.quantity.Multiply(*order.request.limit_price);
    if (quote.ok()) update.traded_value = *quote;
  }
  events_.emplace_back(std::move(update));
}

void SimpleSimulatedExchange::EmitBalance(const AssetId& asset) {
  Balance balance;
  balance.account = config_.account;
  balance.asset = asset;
  balance.total = BalanceOf(asset);
  balance.available = AvailableBalance(asset);
  balance.time = Now();
  events_.emplace_back(std::move(balance));
}

absl::Status SimpleSimulatedExchange::ValidateAndQuantize(
    ApprovedOrder* approved) const {
  auto& request = approved->request;
  if (!approved->strategy_id.IsValid())
    return Error(ErrorCode::kOrderStrategyIdInvalid, "invalid strategy ID");
  if (request.account != config_.account)
    return Error(ErrorCode::kOrderAccountInvalid,
                 "order account is not the simulated account");
  if (request.market != config_.market.market)
    return Error(ErrorCode::kOrderMarketInvalid,
                 "order market is not the simulated market");
  if (request.type != OrderType::Limit && request.type != OrderType::LimitMaker)
    return Error(ErrorCode::kOrderTypeUnsupported, "only limit orders");
  if (!request.limit_price || !request.quantity.IsStrictlyPositive() ||
      !request.limit_price->IsStrictlyPositive())
    return Error(ErrorCode::kOrderPriceOrAmountInvalid,
                 "limit price and amount must be positive");
  auto amount = request.quantity.Quantize(config_.trading_rule.base_increment,
                                          RoundingMode::Down);
  auto price = request.limit_price->Quantize(
      config_.trading_rule.price_increment, RoundingMode::Down);
  if (!amount.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated quantity cannot be quantized to trading rule");
  if (!price.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated price cannot be quantized to trading rule");
  if (!price->IsStrictlyPositive())
    return Error(ErrorCode::kOrderPriceOrAmountInvalid,
                 "price is below one price increment");
  if (!amount->IsStrictlyPositive() ||
      !AtLeast(*amount, config_.trading_rule.min_base_amount)) {
    return Error(ErrorCode::kOrderBelowMinAmount,
                 "Simulated order below minimum amount");
  }
  auto order_value = amount->Multiply(*price);
  if (!order_value.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated order value cannot be calculated");
  if (!AtLeast(*order_value, config_.trading_rule.min_order_value)) {
    return Error(ErrorCode::kOrderBelowMinOrderValue,
                 "Simulated order below minimum order value");
  }
  if (config_.trading_rule.max_base_amount &&
      !AtLeast(*config_.trading_rule.max_base_amount, *amount)) {
    return Error(ErrorCode::kOrderAboveMaxAmount,
                 "Simulated order above max amount");
  }
  request.quantity = *amount;
  request.limit_price = *price;
  return absl::OkStatus();
}

absl::StatusOr<PreparedOrder> SimpleSimulatedExchange::PrepareSubmit(
    ApprovedOrder approved) {
  auto status = ValidateAndQuantize(&approved);
  if (!status.ok()) return status;
  ClientOrderId id =
      config_.make_client_id
          ? config_.make_client_id(approved.request.side)
          : ClientOrderId("P" + std::to_string(next_client_id_++));
  if (id.value.empty() || used_ids_.contains(id.value)) {
    return Error(ErrorCode::kOrderDuplicate, "duplicate Simulated client ID");
  }
  PreparedOrder prepared;
  prepared.client_order_id = id;
  prepared.strategy_id = approved.strategy_id;
  prepared.request = approved.request;
  prepared.created_at_utc = clock_.UtcNow();
  used_ids_.insert(id.value);
  prepared_.emplace(id.value, std::move(approved));
  return prepared;
}

absl::Status SimpleSimulatedExchange::StartPrepared(
    const ClientOrderId& client_order_id) {
  auto it = prepared_.find(client_order_id.value);
  if (it == prepared_.end())
    return Error(ErrorCode::kOrderNotFound, "Simulated prepared order absent");
  ApprovedOrder approved = std::move(it->second);
  prepared_.erase(it);
  RestingOrder order{client_order_id, approved.strategy_id, approved.request};
  const AssetId& collateral = order.request.side == Side::Buy
                                  ? config_.market.quote_asset
                                  : config_.market.base_asset;
  auto required =
      order.request.side == Side::Buy
          ? order.request.quantity.Multiply(*order.request.limit_price)
          : absl::StatusOr<Decimal>(order.request.quantity);
  if (!required.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated order collateral cannot be calculated");
  if (order.request.side == Side::Buy && !config_.buy_fee_from_returns) {
    auto fee = required->Multiply(config_.maker_fee_rate);
    if (!fee.ok())
      return Error(ErrorCode::kDecimalArithmeticFailed,
                   "Simulated fee calculation failed");
    required = required->Add(*fee);
    if (!required.ok())
      return Error(ErrorCode::kDecimalArithmeticFailed,
                   "Simulated order fee-adjusted collateral failed");
  }
  if (!AtLeast(AvailableBalance(collateral), *required)) {
    EmitOrder(order, ExchangeOrderStatus::Rejected);
    return Error(ErrorCode::kSimulatedBalanceInsufficient,
                 "Simulated available balance insufficient");
  }
  orders_.push_back(order);
  EmitOrder(order, ExchangeOrderStatus::Open);
  EmitBalance(collateral);
  return absl::OkStatus();
}

absl::Status SimpleSimulatedExchange::AbortPrepared(
    const ClientOrderId& client_order_id) {
  if (prepared_.erase(client_order_id.value) == 0) {
    return Error(ErrorCode::kOrderNotFound, "Simulated prepared order absent");
  }
  return absl::OkStatus();
}

absl::Status SimpleSimulatedExchange::StartCancel(
    const StrategyId& strategy_id, const ClientOrderId& client_order_id) {
  auto it = std::find_if(orders_.begin(), orders_.end(),
                         [&](const RestingOrder& order) {
                           return order.client_order_id == client_order_id &&
                                  order.strategy_id == strategy_id;
                         });
  if (it == orders_.end())
    return Error(ErrorCode::kOrderNotFound, "Simulated open order absent");
  RestingOrder order = *it;
  orders_.erase(it);
  EmitOrder(order, ExchangeOrderStatus::Canceled);
  EmitBalance(order.request.side == Side::Buy ? config_.market.quote_asset
                                              : config_.market.base_asset);
  return absl::OkStatus();
}

absl::Status SimpleSimulatedExchange::Fill(size_t index) {
  RestingOrder order = orders_.at(index);
  const Decimal& price = *order.request.limit_price;
  const Decimal& amount = order.request.quantity;
  auto quote = price.Multiply(amount);
  if (!quote.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated fill quote calculation failed");
  const bool buy = order.request.side == Side::Buy;
  AssetId fee_asset = buy && config_.buy_fee_from_returns
                          ? config_.market.base_asset
                          : config_.market.quote_asset;
  auto fee_base = buy && config_.buy_fee_from_returns
                      ? absl::StatusOr<Decimal>(amount)
                      : absl::StatusOr<Decimal>(*quote);
  auto fee = fee_base->Multiply(config_.maker_fee_rate);
  if (!fee.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated fill fee calculation failed");

  auto next_base = buy ? BalanceOf(config_.market.base_asset).Add(amount)
                       : BalanceOf(config_.market.base_asset).Subtract(amount);
  auto next_quote = buy ? BalanceOf(config_.market.quote_asset).Subtract(*quote)
                        : BalanceOf(config_.market.quote_asset).Add(*quote);
  if (!next_base.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated fill base balance failed");
  if (!next_quote.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated fill quote balance failed");
  if (fee_asset == config_.market.base_asset)
    next_base = next_base->Subtract(*fee);
  else
    next_quote = next_quote->Subtract(*fee);
  if (!next_base.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated fill net base balance failed");
  if (!next_quote.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated fill net quote balance failed");
  auto fees_total = FeesPaid(fee_asset).Add(*fee);
  if (!fees_total.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Simulated accumulated fees failed");

  orders_.erase(orders_.begin() + index);
  balances_[config_.market.base_asset.value] = *next_base;
  balances_[config_.market.quote_asset.value] = *next_quote;
  fees_paid_[fee_asset.value] = *fees_total;

  TradeUpdate trade;
  trade.account = config_.account;
  trade.market = config_.market.market;
  trade.client_order_id = order.client_order_id;
  trade.exchange_trade_id =
      ExchangeTradeId("T" + std::to_string(next_trade_id_++));
  trade.price = price;
  trade.quantity = amount;
  trade.value = *quote;
  trade.fees.push_back(TradeFee{fee_asset, *fee});
  trade.maker = true;
  trade.time = Now();
  events_.emplace_back(std::move(trade));
  EmitOrder(order, ExchangeOrderStatus::Traded);
  EmitBalance(config_.market.base_asset);
  EmitBalance(config_.market.quote_asset);
  return absl::OkStatus();
}

absl::Status SimpleSimulatedExchange::OnBookBbo(const Decimal& bid,
                                                const Decimal& ask) {
  if (!bid.IsStrictlyPositive() || !ask.IsStrictlyPositive()) {
    return Error(ErrorCode::kSimulatedMarketDataInvalid,
                 "Simulated BBO invalid");
  }
  for (size_t i = 0; i < orders_.size();) {
    const auto& order = orders_[i];
    const auto match = order.request.side == Side::Buy
                           ? order.request.limit_price->Compare(ask)
                           : order.request.limit_price->Compare(bid);
    if (!match.ok())
      return Error(ErrorCode::kSimulatedMarketDataInvalid,
                   "Simulated BBO price comparison failed");
    const bool touched =
        order.request.side == Side::Buy ? *match >= 0 : *match <= 0;
    if (touched) {
      auto status = Fill(i);
      if (!status.ok()) return status;
    } else
      ++i;
  }
  return absl::OkStatus();
}

absl::Status SimpleSimulatedExchange::OnPublicTrade(
    Side aggressor, const Decimal& price, const Decimal& public_amount) {
  if (!price.IsStrictlyPositive() || !public_amount.IsStrictlyPositive()) {
    return Error(ErrorCode::kSimulatedMarketDataInvalid,
                 "Simulated public trade invalid");
  }
  for (size_t i = 0; i < orders_.size();) {
    const auto& order = orders_[i];
    if (order.request.side == aggressor) {
      ++i;
      continue;
    }
    auto comparison = order.request.limit_price->Compare(price);
    if (!comparison.ok())
      return Error(ErrorCode::kSimulatedMarketDataInvalid,
                   "Simulated public trade price comparison failed");
    const bool crossed =
        order.request.side == Side::Buy ? *comparison > 0 : *comparison < 0;
    if (crossed) {
      auto status = Fill(i);
      if (!status.ok()) return status;
    } else
      ++i;
  }
  return absl::OkStatus();
}

}  // namespace hquant
