#include "order/binance_spot_account.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "base/error.h"
#include "base/types.h"
#include "simdjson.h"

namespace hquant::binance_spot::private_detail {

inline absl::Status Invalid(std::string_view detail) {
  return Error(ErrorCode::kAccountMessageInvalid, detail);
}

inline absl::StatusOr<std::string_view> Text(simdjson::dom::element object,
                                             const char* field) {
  std::string_view text;
  if (object[field].get(text)) {
    return Invalid(std::string("missing string field ") + field);
  }
  return text;
}

inline absl::StatusOr<int64_t> Integer(simdjson::dom::element object,
                                       const char* field) {
  int64_t number;
  if (object[field].get(number)) {
    return Invalid(std::string("missing integer field ") + field);
  }
  return number;
}

inline absl::StatusOr<bool> Boolean(simdjson::dom::element object,
                                    const char* field) {
  bool value;
  if (object[field].get(value)) {
    return Invalid(std::string("missing boolean field ") + field);
  }
  return value;
}

inline absl::StatusOr<Decimal> Amount(simdjson::dom::element object,
                                      const char* field,
                                      bool positive = false) {
  auto raw = Text(object, field);
  if (!raw.ok()) return raw.status();
  auto value = Decimal::Parse(*raw);
  if (!value.ok()) return value.status();
  if (positive ? !value->IsStrictlyPositive() : !value->IsNonnegative()) {
    return Invalid(std::string("invalid amount field ") + field);
  }
  return *value;
}

inline absl::StatusOr<UtcTime> Millis(int64_t value) {
  if (value < 0 || value > std::numeric_limits<int64_t>::max() / 1000) {
    return Invalid("timestamp out of range");
  }
  return UtcTime(std::chrono::microseconds(value * 1000));
}

inline absl::StatusOr<simdjson::dom::element> Root(
    simdjson::dom::parser& parser, std::string_view json) {
  simdjson::dom::element root;
  if (parser.parse(json).get(root)) return Invalid("invalid JSON");
  simdjson::dom::element data;
  if (!root["data"].get(data)) return data;  // combined stream wrapper
  return root;
}

}  // namespace hquant::binance_spot::private_detail

namespace hquant::binance_spot {
namespace {
namespace field = private_detail;

absl::StatusOr<ExchangeOrderStatus> Status(std::string_view raw) {
  if (raw == "NEW" || raw == "PENDING_CANCEL") return ExchangeOrderStatus::Open;
  if (raw == "PARTIALLY_FILLED") return ExchangeOrderStatus::PartiallyTraded;
  if (raw == "FILLED") return ExchangeOrderStatus::Traded;
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
                                              const ExchangeId& exchange,
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

  const MarketId market{exchange, InstrumentKind::Spot, std::string(*symbol)};
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
  if (account_.value.empty() || exchange_.value.empty()) {
    return field::Invalid("invalid user data stream identity");
  }
  simdjson::dom::parser parser;
  auto root = field::Root(parser, json);
  if (!root.ok()) return root.status();
  auto type = field::Text(*root, "e");
  if (!type.ok()) return type.status();
  if (*type == "executionReport") {
    auto batch = ExecutionReport(*root, account_, exchange_, received);
    if (!batch.ok())
      return Error(ErrorCode::kAccountMessageInvalid, batch.status().message());
    return batch;
  }
  if (*type == "outboundAccountPosition") {
    auto batch = AccountPosition(*root, account_, received);
    if (!batch.ok())
      return Error(ErrorCode::kAccountMessageInvalid, batch.status().message());
    return batch;
  }
  if (*type == "eventStreamTerminated") {
    return UserDataBatch{{}, true, true};
  }
  if (*type == "balanceUpdate" || *type == "externalLockUpdate" ||
      *type == "listStatus") {
    // These do not carry a complete account/order state for the domain model.
    return UserDataBatch{{}, false, true};
  }
  return Error(ErrorCode::kAccountEventUnsupported,
               "unsupported Binance user data event");
}

}  // namespace hquant::binance_spot

namespace hquant::binance_spot {
namespace {
namespace field = private_detail;

bool ValidMarket(const MarketId& market) {
  return !market.exchange.value.empty() && !market.native_symbol.empty() &&
         market.instrument_kind == InstrumentKind::Spot;
}

template <typename T>
absl::StatusOr<T> RestResult(absl::StatusOr<T> result) {
  if (result.ok()) return result;
  const ErrorCode code = CodeOf(result.status());
  if (code == ErrorCode::kReconcileIdentityMismatch ||
      code == ErrorCode::kReconcileResponseTooLarge) {
    return result.status();
  }
  return Error(ErrorCode::kReconcileResponseInvalid, result.status().message());
}

std::string Encode(std::string_view raw) {
  constexpr char hex[] = "0123456789ABCDEF";
  std::string encoded;
  encoded.reserve(raw.size());
  for (unsigned char character : raw) {
    if ((character >= 'A' && character <= 'Z') ||
        (character >= 'a' && character <= 'z') ||
        (character >= '0' && character <= '9') || character == '-' ||
        character == '_' || character == '.' || character == '~') {
      encoded.push_back(static_cast<char>(character));
    } else {
      encoded.push_back('%');
      encoded.push_back(hex[character >> 4]);
      encoded.push_back(hex[character & 15]);
    }
  }
  return encoded;
}

absl::StatusOr<ExchangeOrderStatus> ParseStatus(std::string_view raw) {
  if (raw == "NEW" || raw == "PENDING_CANCEL") return ExchangeOrderStatus::Open;
  if (raw == "PARTIALLY_FILLED") return ExchangeOrderStatus::PartiallyTraded;
  if (raw == "FILLED") return ExchangeOrderStatus::Traded;
  if (raw == "CANCELED") return ExchangeOrderStatus::Canceled;
  if (raw == "REJECTED") return ExchangeOrderStatus::Rejected;
  if (raw == "EXPIRED" || raw == "EXPIRED_IN_MATCH") {
    return ExchangeOrderStatus::Expired;
  }
  return field::Invalid("unsupported REST order status");
}

absl::StatusOr<OrderUpdate> ParseOrder(std::string_view json,
                                       const ReconciliationTarget& target,
                                       EventTime received) {
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root)) return field::Invalid("invalid order JSON");
  auto symbol = field::Text(root, "symbol");
  auto client = field::Text(root, "clientOrderId");
  auto id = field::Integer(root, "orderId");
  auto status = field::Text(root, "status");
  auto filled = field::Amount(root, "executedQty");
  auto quote_text = field::Text(root, "cummulativeQuoteQty");
  auto updated = field::Integer(root, "updateTime");
  if (!symbol.ok()) return symbol.status();
  if (!client.ok()) return client.status();
  if (!id.ok()) return id.status();
  if (!status.ok()) return status.status();
  if (!filled.ok()) return filled.status();
  if (!quote_text.ok()) return quote_text.status();
  if (!updated.ok()) return updated.status();
  if (*symbol != target.market.native_symbol ||
      *client != target.original_client_id.value || *id <= 0) {
    return Error(ErrorCode::kReconcileIdentityMismatch,
                 "REST order identity mismatch");
  }
  auto mapped = ParseStatus(*status);
  if (!mapped.ok()) return mapped.status();
  auto utc = field::Millis(*updated);
  if (!utc.ok()) return utc.status();
  received.exchange_utc = *utc;
  OrderUpdate order;
  order.account = target.account;
  order.market = target.market;
  order.client_id = target.original_client_id;
  order.exchange_order_id = ExchangeOrderId(std::to_string(*id));
  order.exchange_status = *mapped;
  order.cumulative_base = *filled;
  // Binance documents negative historical quote totals as unavailable.
  auto quote = Decimal::Parse(*quote_text);
  if (!quote.ok()) return quote.status();
  if (quote->IsNonnegative()) order.cumulative_quote = *quote;
  order.time = received;
  return order;
}

using IdTrade = std::pair<int64_t, TradeUpdate>;

absl::StatusOr<std::vector<IdTrade>> ParseTrades(
    std::string_view json, const ReconciliationTarget& target,
    const ExchangeOrderId& exchange_id, EventTime received) {
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root))
    return field::Invalid("invalid trades JSON");
  simdjson::dom::array items;
  if (root.get(items)) return field::Invalid("trades response is not an array");
  std::vector<IdTrade> trades;
  trades.reserve(items.size());
  for (auto item : items) {
    auto symbol = field::Text(item, "symbol");
    auto trade_id = field::Integer(item, "id");
    auto order_id = field::Integer(item, "orderId");
    auto price = field::Amount(item, "price", true);
    auto base = field::Amount(item, "qty", true);
    auto quote = field::Amount(item, "quoteQty");
    auto commission = field::Amount(item, "commission");
    auto maker = field::Boolean(item, "isMaker");
    auto time = field::Integer(item, "time");
    if (!symbol.ok()) return symbol.status();
    if (!trade_id.ok()) return trade_id.status();
    if (!order_id.ok()) return order_id.status();
    if (!price.ok()) return price.status();
    if (!base.ok()) return base.status();
    if (!quote.ok()) return quote.status();
    if (!commission.ok()) return commission.status();
    if (!maker.ok()) return maker.status();
    if (!time.ok()) return time.status();
    if (*symbol != target.market.native_symbol || *trade_id < 0 ||
        std::to_string(*order_id) != exchange_id.value) {
      return Error(ErrorCode::kReconcileIdentityMismatch,
                   "REST trade identity mismatch");
    }
    auto utc = field::Millis(*time);
    if (!utc.ok()) return utc.status();
    TradeUpdate trade;
    trade.account = target.account;
    trade.market = target.market;
    trade.client_id = target.original_client_id;
    trade.exchange_order_id = exchange_id;
    trade.exchange_trade_id = ExchangeTradeId(std::to_string(*trade_id));
    trade.price = *price;
    trade.base_amount = *base;
    trade.quote_amount = *quote;
    trade.maker = *maker;
    trade.time = received;
    trade.time.exchange_utc = *utc;
    if (commission->IsStrictlyPositive()) {
      auto fee_asset = field::Text(item, "commissionAsset");
      if (!fee_asset.ok() || fee_asset->empty()) {
        return field::Invalid("nonzero trade commission without asset");
      }
      trade.fees.push_back(
          TradeFee{AssetId(std::string(*fee_asset)), *commission});
    }
    trades.emplace_back(*trade_id, std::move(trade));
  }
  return trades;
}

absl::StatusOr<std::vector<ReconciliationTarget>> ParseOpenOrders(
    std::string_view json, const AccountId& account, const MarketId& market) {
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root))
    return field::Invalid("invalid open orders JSON");
  simdjson::dom::array items;
  if (root.get(items))
    return field::Invalid("open orders response is not an array");
  if (items.size() > 8192)
    return Error(ErrorCode::kReconcileResponseTooLarge,
                 "open orders scan too large");
  std::vector<ReconciliationTarget> targets;
  std::set<std::string> seen;
  for (auto item : items) {
    auto symbol = field::Text(item, "symbol");
    auto client = field::Text(item, "clientOrderId");
    if (!symbol.ok()) return symbol.status();
    if (!client.ok()) return client.status();
    if (*symbol != market.native_symbol || client->empty()) {
      return Error(ErrorCode::kReconcileIdentityMismatch,
                   "open order identity mismatch");
    }
    if (seen.emplace(*client).second) {
      targets.push_back({account, market, ClientOrderId(std::string(*client))});
    }
  }
  return targets;
}

}  // namespace

absl::StatusOr<RestartReconciliationPlan> PlanRestart(
    const RestartReconciliationInput& input) {
  if (input.account.value.empty() || input.assigned_markets.empty()) {
    return Error(ErrorCode::kReconcileTargetInvalid,
                 "restart account or markets missing");
  }
  std::set<std::string> markets;
  for (const auto& market : input.assigned_markets) {
    if (!ValidMarket(market) || !markets.emplace(market.native_symbol).second) {
      return Error(ErrorCode::kReconcileTargetInvalid,
                   "invalid or duplicate restart market");
    }
  }
  std::map<std::string, ReconciliationTarget> by_client_id;
  auto insert = [&](const AccountId& account, const MarketId& market,
                    const ClientOrderId& client_id) -> absl::Status {
    if (account != input.account || !markets.contains(market.native_symbol) ||
        std::find(input.assigned_markets.begin(), input.assigned_markets.end(),
                  market) == input.assigned_markets.end() ||
        !ValidMarket(market) || client_id.value.empty()) {
      return Error(ErrorCode::kReconcileTargetInvalid,
                   "restart order outside account assignment");
    }
    auto [it, inserted] = by_client_id.emplace(
        client_id.value, ReconciliationTarget{account, market, client_id});
    if (!inserted && it->second.market != market) {
      return Error(ErrorCode::kReconcileIdentityMismatch,
                   "client ID assigned to two markets");
    }
    return absl::OkStatus();
  };
  for (const auto& prepared : input.persisted_prepared_orders) {
    auto status = insert(prepared.request.account, prepared.request.market,
                         prepared.client_id);
    if (!status.ok()) return status;
  }
  for (const auto& snapshot : input.live_snapshots) {
    auto status = insert(snapshot.request.account, snapshot.request.market,
                         snapshot.client_id);
    if (!status.ok()) return status;
  }
  RestartReconciliationPlan plan;
  for (const auto& [_, target] : by_client_id)
    plan.known_orders.push_back(target);
  if (!input.history_complete || !input.previous_run_clean) {
    plan.markets_to_scan = input.assigned_markets;
  }
  plan.pause_stateful_executors = !input.history_complete ||
                                  !input.previous_run_clean ||
                                  !input.executor_checkpoints_complete;
  return plan;
}

boost::asio::awaitable<absl::StatusOr<ReconciliationBatch>>
ReconciliationClient::Query(ReconciliationTarget target, EventTime received,
                            std::chrono::steady_clock::time_point deadline) {
  if (target.account.value.empty() || !ValidMarket(target.market) ||
      target.original_client_id.value.empty() || limits_.max_trade_pages == 0 ||
      limits_.trades_per_page == 0 || limits_.trades_per_page > 1000 ||
      limits_.max_response_bytes == 0) {
    co_return Error(ErrorCode::kReconcileTargetInvalid,
                    "invalid reconciliation target or limits");
  }
  ReconciliationBatch batch;
  batch.target = target;
  const std::string order_path =
      "/api/v3/order?symbol=" + Encode(target.market.native_symbol) +
      "&origClientOrderId=" + Encode(target.original_client_id.value);
  auto order_response = co_await rest_.GetSigned(order_path, deadline);
  if (!order_response.ok()) {
    batch.unresolved_status = Error(ErrorCode::kReconcileQueryFailed,
                                    order_response.status().message());
    co_return batch;
  }
  if (order_response->status != 200) {
    // Even Binance -2013 / HTTP 404 cannot prove an uncertain write was not
    // accepted; orders can age out of the query window.
    batch.unresolved_status = Error(
        ErrorCode::kReconcileQueryFailed,
        "order query returned HTTP " + std::to_string(order_response->status));
    co_return batch;
  }
  if (order_response->body.size() > limits_.max_response_bytes) {
    batch.unresolved_status = Error(ErrorCode::kReconcileResponseTooLarge,
                                    "order response exceeds size limit");
    co_return batch;
  }
  auto order = RestResult(ParseOrder(order_response->body, target, received));
  if (!order.ok()) {
    batch.unresolved_status = order.status();
    co_return batch;
  }
  const auto& exchange_id = *order->exchange_order_id;
  std::map<int64_t, TradeUpdate> by_trade_id;
  std::optional<int64_t> from_id;
  bool exhausted = false;
  for (size_t page = 0; page < limits_.max_trade_pages; ++page) {
    std::string path =
        "/api/v3/myTrades?symbol=" + Encode(target.market.native_symbol) +
        "&orderId=" + Encode(exchange_id.value) +
        "&limit=" + std::to_string(limits_.trades_per_page);
    if (from_id) path += "&fromId=" + std::to_string(*from_id);
    auto response = co_await rest_.GetSigned(std::move(path), deadline);
    if (!response.ok()) {
      batch.unresolved_status =
          Error(ErrorCode::kReconcileQueryFailed, response.status().message());
      co_return batch;
    }
    if (response->status != 200) {
      batch.unresolved_status =
          Error(ErrorCode::kReconcileQueryFailed, "trade query failed");
      co_return batch;
    }
    if (response->body.size() > limits_.max_response_bytes) {
      batch.unresolved_status = Error(ErrorCode::kReconcileResponseTooLarge,
                                      "trade response too large");
      co_return batch;
    }
    auto trades =
        RestResult(ParseTrades(response->body, target, exchange_id, received));
    if (!trades.ok()) {
      batch.unresolved_status = trades.status();
      co_return batch;
    }
    int64_t highest = -1;
    for (auto& [id, trade] : *trades) {
      highest = std::max(highest, id);
      auto [it, inserted] = by_trade_id.emplace(id, std::move(trade));
      if (!inserted) {
        batch.unresolved_status = Error(ErrorCode::kReconcileResponseInvalid,
                                        "duplicate trade ID in REST pages");
        co_return batch;
      }
    }
    // Preserve verified partial fills if a later page fails. Incomplete never
    // permits releasing the unknown order's remaining funds hold.
    batch.trades.clear();
    for (const auto& [_, trade] : by_trade_id) batch.trades.push_back(trade);
    if (trades->size() < limits_.trades_per_page) {
      exhausted = true;
      break;
    }
    if (highest == std::numeric_limits<int64_t>::max() ||
        (from_id && highest < *from_id)) {
      batch.unresolved_status = Error(ErrorCode::kReconcileResponseInvalid,
                                      "trade pagination made no progress");
      co_return batch;
    }
    from_id = highest + 1;
  }
  if (!exhausted) {
    batch.unresolved_status = Error(ErrorCode::kReconcileResponseTooLarge,
                                    "trade query hit page limit");
    co_return batch;
  }
  Decimal base_total;
  Decimal quote_total;
  for (const auto& trade : batch.trades) {
    auto base = base_total.Add(trade.base_amount);
    auto quote = quote_total.Add(trade.quote_amount);
    if (!base.ok() || !quote.ok()) {
      batch.unresolved_status =
          Error(ErrorCode::kReconcileResponseInvalid, "trade total overflow");
      co_return batch;
    }
    base_total = *base;
    quote_total = *quote;
  }
  auto same_base = base_total.Compare(*order->cumulative_base);
  if (!same_base.ok() || *same_base != 0) {
    batch.unresolved_status = Error(ErrorCode::kReconcileIdentityMismatch,
                                    "order and trade base totals disagree");
    co_return batch;
  }
  if (order->cumulative_quote) {
    auto same_quote = quote_total.Compare(*order->cumulative_quote);
    if (!same_quote.ok() || *same_quote != 0) {
      batch.unresolved_status = Error(ErrorCode::kReconcileIdentityMismatch,
                                      "order and trade quote totals disagree");
      co_return batch;
    }
  }
  // Publishing a status while fills are incomplete could clear Tracker's
  // SubmissionUnknown flag. Only a complete comparison releases that gate.
  batch.order = *order;
  batch.complete = true;
  co_return batch;
}

boost::asio::awaitable<absl::StatusOr<std::vector<ReconciliationTarget>>>
ReconciliationClient::DiscoverOpenOrders(
    AccountId account, MarketId market,
    std::chrono::steady_clock::time_point deadline) {
  if (account.value.empty() || !ValidMarket(market) ||
      limits_.max_response_bytes == 0) {
    co_return Error(ErrorCode::kReconcileTargetInvalid,
                    "invalid open order scan target");
  }
  const std::string path =
      "/api/v3/openOrders?symbol=" + Encode(market.native_symbol);
  auto response = co_await rest_.GetSigned(path, deadline);
  if (!response.ok())
    co_return Error(ErrorCode::kReconcileQueryFailed,
                    response.status().message());
  if (response->status != 200) {
    co_return Error(ErrorCode::kReconcileQueryFailed,
                    "open orders query failed");
  }
  if (response->body.size() > limits_.max_response_bytes) {
    co_return Error(ErrorCode::kReconcileResponseTooLarge,
                    "open orders response too large");
  }
  co_return RestResult(ParseOpenOrders(response->body, account, market));
}

boost::asio::awaitable<absl::StatusOr<std::vector<ReconciliationTarget>>>
ReconciliationClient::DiscoverRecentOrders(
    AccountId account, MarketId market, UtcTime start, UtcTime end,
    std::chrono::steady_clock::time_point deadline) {
  if (account.value.empty() || !ValidMarket(market) || start >= end ||
      end - start > std::chrono::hours(24) ||
      start.time_since_epoch().count() < 0 || limits_.max_response_bytes == 0) {
    co_return Error(ErrorCode::kReconcileTargetInvalid,
                    "invalid recent order scan window");
  }
  const auto start_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            start.time_since_epoch())
                            .count();
  const auto end_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          end.time_since_epoch())
                          .count();
  if (start_ms == end_ms) {
    co_return Error(ErrorCode::kReconcileTargetInvalid,
                    "recent order scan window too narrow");
  }
  const std::string path =
      "/api/v3/allOrders?symbol=" + Encode(market.native_symbol) +
      "&startTime=" + std::to_string(start_ms) +
      "&endTime=" + std::to_string(end_ms) + "&limit=1000";
  auto response = co_await rest_.GetSigned(path, deadline);
  if (!response.ok())
    co_return Error(ErrorCode::kReconcileQueryFailed,
                    response.status().message());
  if (response->status != 200) {
    co_return Error(ErrorCode::kReconcileQueryFailed,
                    "recent orders query failed");
  }
  if (response->body.size() > limits_.max_response_bytes) {
    co_return Error(ErrorCode::kReconcileResponseTooLarge,
                    "recent orders response too large");
  }
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(response->body).get(root)) {
    co_return Error(ErrorCode::kReconcileResponseInvalid,
                    "invalid recent orders JSON");
  }
  simdjson::dom::array orders;
  if (root.get(orders)) {
    co_return Error(ErrorCode::kReconcileResponseInvalid,
                    "recent orders response is not an array");
  }
  if (orders.size() >= 1000) {
    co_return Error(ErrorCode::kReconcileResponseTooLarge,
                    "recent order window may be truncated; split the window");
  }
  co_return RestResult(ParseOpenOrders(response->body, account, market));
}

}  // namespace hquant::binance_spot
