#include "hbot/connector/binance_spot/private/user_data_stream.h"

#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "hbot/connector/binance_spot/private/json_fields.h"
#include "simdjson.h"

namespace hbot::binance_spot {
namespace {
namespace field = private_detail;

absl::StatusOr<ExchangeOrderStatus> Status(std::string_view raw) {
  if (raw == "NEW" || raw == "PENDING_CANCEL") return ExchangeOrderStatus::New;
  if (raw == "PARTIALLY_FILLED") return ExchangeOrderStatus::PartiallyFilled;
  if (raw == "FILLED") return ExchangeOrderStatus::Filled;
  if (raw == "CANCELED") return ExchangeOrderStatus::Canceled;
  if (raw == "REJECTED") return ExchangeOrderStatus::Rejected;
  if (raw == "EXPIRED" || raw == "EXPIRED_IN_MATCH") {
    return ExchangeOrderStatus::Expired;
  }
  return field::Invalid("unsupported executionReport order status");
}

absl::StatusOr<EventTime> ReportTime(simdjson::dom::element data,
                                     EventTime received) {
  auto transaction = field::Integer(data, "T");
  if (!transaction.ok()) transaction = field::Integer(data, "E");
  if (!transaction.ok()) return transaction.status();
  auto utc = field::Millis(*transaction);
  if (!utc.ok()) return utc.status();
  received.exchange_utc = *utc;
  return received;
}

absl::StatusOr<UserDataBatch> ExecutionReport(simdjson::dom::element data,
                                              const AccountId& account,
                                              const VenueId& venue,
                                              EventTime received) {
  auto symbol = field::Text(data, "s");
  auto client = field::Text(data, "c");
  auto execution = field::Text(data, "x");
  auto raw_status = field::Text(data, "X");
  auto order_id = field::Integer(data, "i");
  auto cumulative_base = field::Amount(data, "z");
  auto cumulative_quote = field::Amount(data, "Z");
  auto time = ReportTime(data, received);
  if (!symbol.ok()) return symbol.status();
  if (!client.ok()) return client.status();
  if (!execution.ok()) return execution.status();
  if (!raw_status.ok()) return raw_status.status();
  if (!order_id.ok()) return order_id.status();
  if (!cumulative_base.ok()) return cumulative_base.status();
  if (!cumulative_quote.ok()) return cumulative_quote.status();
  if (!time.ok()) return time.status();
  if (symbol->empty() || (*order_id < 0 && *order_id != -1)) {
    return field::Invalid("invalid executionReport order identity");
  }
  // On cancel, c can identify the cancel request; C is the original order ID.
  std::string_view original = *client;
  if (*execution == "CANCELED" || *execution == "REPLACED") {
    auto canceled_id = field::Text(data, "C");
    if (!canceled_id.ok() || canceled_id->empty()) {
      return field::Invalid("cancel report lacks original client order ID");
    }
    original = *canceled_id;
  }
  if (original.empty())
    return field::Invalid("missing original client order ID");
  auto mapped_status = Status(*raw_status);
  if (!mapped_status.ok()) return mapped_status.status();

  const MarketId market{venue, InstrumentKind::Spot, std::string(*symbol)};
  const ClientOrderId client_id{std::string(original)};
  std::optional<ExchangeOrderId> exchange_id;
  if (*order_id > 0) exchange_id = ExchangeOrderId(std::to_string(*order_id));
  UserDataBatch batch;
  if (*execution == "TRADE") {
    auto trade_id = field::Integer(data, "t");
    auto price = field::Amount(data, "L", true);
    auto base = field::Amount(data, "l", true);
    auto quote = field::Amount(data, "Y");
    auto maker = field::Boolean(data, "m");
    auto commission = field::Amount(data, "n");
    if (!trade_id.ok()) return trade_id.status();
    if (!price.ok()) return price.status();
    if (!base.ok()) return base.status();
    if (!quote.ok()) return quote.status();
    if (!maker.ok()) return maker.status();
    if (!commission.ok()) return commission.status();
    if (*trade_id < 0 || !exchange_id) {
      return field::Invalid("trade without exchange order and trade IDs");
    }
    TradeUpdate trade;
    trade.account = account;
    trade.market = market;
    trade.client_id = client_id;
    trade.exchange_order_id = exchange_id;
    trade.exchange_trade_id = ExchangeTradeId(std::to_string(*trade_id));
    trade.price = *price;
    trade.base_amount = *base;
    trade.quote_amount = *quote;
    trade.maker = *maker;
    trade.time = *time;
    if (commission->IsStrictlyPositive()) {
      auto fee_asset = field::Text(data, "N");
      if (!fee_asset.ok() || fee_asset->empty()) {
        return field::Invalid("nonzero commission without asset");
      }
      trade.fees.push_back(
          TradeFee{AssetId(std::string(*fee_asset)), *commission});
    }
    batch.events.emplace_back(std::move(trade));
  }

  OrderUpdate order;
  order.account = account;
  order.market = market;
  order.client_id = client_id;
  order.exchange_order_id = exchange_id;
  order.exchange_status = *mapped_status;
  order.cumulative_base = *cumulative_base;
  order.cumulative_quote = *cumulative_quote;
  order.time = *time;
  batch.events.emplace_back(std::move(order));
  return batch;
}

absl::StatusOr<UserDataBatch> AccountPosition(simdjson::dom::element data,
                                              const AccountId& account,
                                              EventTime received) {
  auto time = ReportTime(data, received);
  if (!time.ok()) return time.status();
  simdjson::dom::array balances;
  if (data["B"].get(balances))
    return field::Invalid("missing account balances");
  UserDataBatch batch;
  for (auto item : balances) {
    auto asset = field::Text(item, "a");
    auto free = field::Amount(item, "f");
    auto locked = field::Amount(item, "l");
    if (!asset.ok()) return asset.status();
    if (!free.ok()) return free.status();
    if (!locked.ok()) return locked.status();
    if (asset->empty()) return field::Invalid("empty balance asset");
    auto total = free->Add(*locked);
    if (!total.ok()) return total.status();
    batch.events.emplace_back(BalanceUpdate{
        account, AssetId(std::string(*asset)), *total, *free, *time});
  }
  return batch;
}

}  // namespace

absl::StatusOr<UserDataBatch> UserDataStream::Parse(std::string_view json,
                                                    EventTime received) const {
  if (account_.value.empty() || venue_.value.empty()) {
    return field::Invalid("invalid user data stream identity");
  }
  simdjson::dom::parser parser;
  auto root = field::Root(parser, json);
  if (!root.ok()) return root.status();
  auto type = field::Text(*root, "e");
  if (!type.ok()) return type.status();
  if (*type == "executionReport") {
    return ExecutionReport(*root, account_, venue_, received);
  }
  if (*type == "outboundAccountPosition") {
    return AccountPosition(*root, account_, received);
  }
  if (*type == "eventStreamTerminated") {
    return UserDataBatch{{}, true, true};
  }
  if (*type == "balanceUpdate" || *type == "externalLockUpdate" ||
      *type == "listStatus") {
    // These do not carry a complete account/order state for the domain model.
    return UserDataBatch{{}, false, true};
  }
  return absl::UnimplementedError("unsupported Binance user data event");
}

}  // namespace hbot::binance_spot
