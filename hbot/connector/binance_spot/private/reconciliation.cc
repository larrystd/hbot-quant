#include "hbot/connector/binance_spot/private/reconciliation.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "hbot/connector/binance_spot/private/json_fields.h"
#include "simdjson.h"

namespace hbot::binance_spot {
namespace {
namespace field = private_detail;

bool ValidMarket(const MarketId& market) {
  return !market.venue.value.empty() && !market.native_symbol.empty() &&
         market.instrument_kind == InstrumentKind::Spot;
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
  if (raw == "NEW" || raw == "PENDING_CANCEL") return ExchangeOrderStatus::New;
  if (raw == "PARTIALLY_FILLED") return ExchangeOrderStatus::PartiallyFilled;
  if (raw == "FILLED") return ExchangeOrderStatus::Filled;
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
    return absl::FailedPreconditionError("REST order identity mismatch");
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
      return absl::FailedPreconditionError("REST trade identity mismatch");
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
    return absl::ResourceExhaustedError("open orders scan too large");
  std::vector<ReconciliationTarget> targets;
  std::set<std::string> seen;
  for (auto item : items) {
    auto symbol = field::Text(item, "symbol");
    auto client = field::Text(item, "clientOrderId");
    if (!symbol.ok()) return symbol.status();
    if (!client.ok()) return client.status();
    if (*symbol != market.native_symbol || client->empty()) {
      return absl::FailedPreconditionError("open order identity mismatch");
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
    return field::Invalid("restart account or markets missing");
  }
  std::set<std::string> markets;
  for (const auto& market : input.assigned_markets) {
    if (!ValidMarket(market) || !markets.emplace(market.native_symbol).second) {
      return field::Invalid("invalid or duplicate restart market");
    }
  }
  std::map<std::string, ReconciliationTarget> by_client_id;
  auto insert = [&](const AccountId& account, const MarketId& market,
                    const ClientOrderId& client_id) -> absl::Status {
    if (account != input.account || !markets.contains(market.native_symbol) ||
        std::find(input.assigned_markets.begin(), input.assigned_markets.end(),
                  market) == input.assigned_markets.end() ||
        !ValidMarket(market) || client_id.value.empty()) {
      return field::Invalid("restart order outside account assignment");
    }
    auto [it, inserted] = by_client_id.emplace(
        client_id.value, ReconciliationTarget{account, market, client_id});
    if (!inserted && it->second.market != market) {
      return absl::FailedPreconditionError("client ID assigned to two markets");
    }
    return absl::OkStatus();
  };
  for (const auto& intent : input.persisted_intents) {
    auto status =
        insert(intent.request.account, intent.request.market, intent.client_id);
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
    co_return field::Invalid("invalid reconciliation target or limits");
  }
  ReconciliationBatch batch;
  batch.target = target;
  const std::string order_path =
      "/api/v3/order?symbol=" + Encode(target.market.native_symbol) +
      "&origClientOrderId=" + Encode(target.original_client_id.value);
  auto order_response = co_await rest_.GetSigned(order_path, deadline);
  if (!order_response.ok()) {
    batch.unresolved_reason = std::string(order_response.status().message());
    co_return batch;
  }
  if (order_response->status != 200) {
    // Even Binance -2013 / HTTP 404 cannot prove an uncertain write was not
    // accepted; orders can age out of the query window.
    batch.unresolved_reason =
        "order query returned HTTP " + std::to_string(order_response->status);
    co_return batch;
  }
  if (order_response->body.size() > limits_.max_response_bytes) {
    batch.unresolved_reason = "order response exceeds size limit";
    co_return batch;
  }
  auto order = ParseOrder(order_response->body, target, received);
  if (!order.ok()) {
    batch.unresolved_reason = std::string(order.status().message());
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
      batch.unresolved_reason = std::string(response.status().message());
      co_return batch;
    }
    if (response->status != 200 ||
        response->body.size() > limits_.max_response_bytes) {
      batch.unresolved_reason = "trade query failed or response too large";
      co_return batch;
    }
    auto trades = ParseTrades(response->body, target, exchange_id, received);
    if (!trades.ok()) {
      batch.unresolved_reason = std::string(trades.status().message());
      co_return batch;
    }
    int64_t highest = -1;
    for (auto& [id, trade] : *trades) {
      highest = std::max(highest, id);
      auto [it, inserted] = by_trade_id.emplace(id, std::move(trade));
      if (!inserted) {
        batch.unresolved_reason = "duplicate trade ID in REST pages";
        co_return batch;
      }
    }
    // Preserve verified partial fills if a later page fails. Incomplete never
    // permits releasing the unknown order's remaining risk reservation.
    batch.trades.clear();
    for (const auto& [_, trade] : by_trade_id) batch.trades.push_back(trade);
    if (trades->size() < limits_.trades_per_page) {
      exhausted = true;
      break;
    }
    if (highest == std::numeric_limits<int64_t>::max() ||
        (from_id && highest < *from_id)) {
      batch.unresolved_reason = "trade pagination made no progress";
      co_return batch;
    }
    from_id = highest + 1;
  }
  if (!exhausted) {
    batch.unresolved_reason = "trade query hit page limit";
    co_return batch;
  }
  Decimal base_total;
  Decimal quote_total;
  for (const auto& trade : batch.trades) {
    auto base = base_total.Add(trade.base_amount);
    auto quote = quote_total.Add(trade.quote_amount);
    if (!base.ok() || !quote.ok()) {
      batch.unresolved_reason = "trade total overflow";
      co_return batch;
    }
    base_total = *base;
    quote_total = *quote;
  }
  auto same_base = base_total.Compare(*order->cumulative_base);
  if (!same_base.ok() || *same_base != 0) {
    batch.unresolved_reason = "order and trade base totals disagree";
    co_return batch;
  }
  if (order->cumulative_quote) {
    auto same_quote = quote_total.Compare(*order->cumulative_quote);
    if (!same_quote.ok() || *same_quote != 0) {
      batch.unresolved_reason = "order and trade quote totals disagree";
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
    co_return field::Invalid("invalid open order scan target");
  }
  const std::string path =
      "/api/v3/openOrders?symbol=" + Encode(market.native_symbol);
  auto response = co_await rest_.GetSigned(path, deadline);
  if (!response.ok()) co_return response.status();
  if (response->status != 200) {
    co_return absl::UnavailableError("open orders query failed");
  }
  if (response->body.size() > limits_.max_response_bytes) {
    co_return absl::ResourceExhaustedError("open orders response too large");
  }
  co_return ParseOpenOrders(response->body, account, market);
}

boost::asio::awaitable<absl::StatusOr<std::vector<ReconciliationTarget>>>
ReconciliationClient::DiscoverRecentOrders(
    AccountId account, MarketId market, UtcTime start, UtcTime end,
    std::chrono::steady_clock::time_point deadline) {
  if (account.value.empty() || !ValidMarket(market) || start >= end ||
      end - start > std::chrono::hours(24) ||
      start.time_since_epoch().count() < 0 || limits_.max_response_bytes == 0) {
    co_return field::Invalid("invalid recent order scan window");
  }
  const auto start_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            start.time_since_epoch())
                            .count();
  const auto end_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          end.time_since_epoch())
                          .count();
  if (start_ms == end_ms) {
    co_return field::Invalid("recent order scan window too narrow");
  }
  const std::string path =
      "/api/v3/allOrders?symbol=" + Encode(market.native_symbol) +
      "&startTime=" + std::to_string(start_ms) +
      "&endTime=" + std::to_string(end_ms) + "&limit=1000";
  auto response = co_await rest_.GetSigned(path, deadline);
  if (!response.ok()) co_return response.status();
  if (response->status != 200) {
    co_return absl::UnavailableError("recent orders query failed");
  }
  if (response->body.size() > limits_.max_response_bytes) {
    co_return absl::ResourceExhaustedError("recent orders response too large");
  }
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(response->body).get(root)) {
    co_return field::Invalid("invalid recent orders JSON");
  }
  simdjson::dom::array orders;
  if (root.get(orders)) {
    co_return field::Invalid("recent orders response is not an array");
  }
  if (orders.size() >= 1000) {
    co_return absl::ResourceExhaustedError(
        "recent order window may be truncated; split the window");
  }
  co_return ParseOpenOrders(response->body, account, market);
}

}  // namespace hbot::binance_spot
