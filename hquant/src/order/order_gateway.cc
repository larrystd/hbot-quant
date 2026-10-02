#include "order/order_gateway.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <climits>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "base/error.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "openssl/evp.h"
#include "openssl/hmac.h"
#include "simdjson.h"

namespace hquant::binance_spot {
namespace {
constexpr std::string_view kAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
constexpr uint32_t kMaxShardSequence = (uint32_t{1} << 29) - 1;

void AppendBase32(uint64_t value, int digits, std::string* output) {
  for (int shift = (digits - 1) * 5; shift >= 0; shift -= 5) {
    output->push_back(kAlphabet[(value >> shift) & 31]);
  }
}

absl::StatusOr<uint64_t> ParseBase32(std::string_view text, uint64_t max) {
  uint64_t value = 0;
  for (char ch : text) {
    const auto digit = kAlphabet.find(ch);
    if (digit == std::string_view::npos || value > (max >> 5)) {
      return Error(ErrorCode::kClientOrderIdInvalid,
                   "invalid client ID base32 digit/overflow");
    }
    value = (value << 5) | digit;
    if (value > max)
      return Error(ErrorCode::kClientOrderIdInvalid,
                   "client ID field overflow");
  }
  return value;
}
}  // namespace

absl::StatusOr<ClientOrderId> EncodeClientOrderId(const StrategyId& strategy_id,
                                                  RunId run, ShardId shard,
                                                  uint32_t shard_sequence) {
  if (!strategy_id.IsValid() || !run.IsValid() || !shard.IsValid() ||
      shard_sequence == 0 || shard_sequence > kMaxShardSequence) {
    return Error(ErrorCode::kClientOrderIdInvalid,
                 "invalid client ID components");
  }
  const uint32_t suffix =
      (static_cast<uint32_t>(shard.value) << 29) | shard_sequence;
  std::string text;
  text.reserve(31);
  text.push_back('H');
  AppendBase32(strategy_id.value, 10, &text);
  AppendBase32(run.value, 13, &text);
  AppendBase32(suffix, 7, &text);
  return ClientOrderId(std::move(text));
}

absl::StatusOr<DecodedClientOrderId> DecodeClientOrderId(
    const ClientOrderId& id) {
  if (id.value.size() != 31 || id.value[0] != 'H') {
    return Error(ErrorCode::kClientOrderIdInvalid,
                 "unsupported client ID format");
  }
  auto strategy_id = ParseBase32(std::string_view(id.value).substr(1, 10),
                                 (uint64_t{1} << 48) - 1);
  auto run = ParseBase32(std::string_view(id.value).substr(11, 13), UINT64_MAX);
  auto suffix =
      ParseBase32(std::string_view(id.value).substr(24, 7), UINT32_MAX);
  if (!strategy_id.ok()) return strategy_id.status();
  if (!run.ok()) return run.status();
  if (!suffix.ok()) return suffix.status();
  DecodedClientOrderId result{
      *strategy_id, RunId{*run}, ShardId{static_cast<uint8_t>(*suffix >> 29)},
      static_cast<uint32_t>(*suffix & kMaxShardSequence)};
  if (result.strategy_id == 0 || !result.run.IsValid() ||
      result.shard_sequence == 0) {
    return Error(ErrorCode::kClientOrderIdInvalid,
                 "invalid client ID identity");
  }
  return result;
}

}  // namespace hquant::binance_spot

namespace hquant::binance_spot {
namespace {

std::string PercentEncode(std::string_view text) {
  constexpr char kHex[] = "0123456789ABCDEF";
  std::string output;
  for (unsigned char ch : text) {
    if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
        (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' ||
        ch == '~') {
      output.push_back(static_cast<char>(ch));
    } else {
      output.push_back('%');
      output.push_back(kHex[ch >> 4]);
      output.push_back(kHex[ch & 15]);
    }
  }
  return output;
}

}  // namespace

absl::StatusOr<std::string> CanonicalQuery(const QueryParameters& parameters) {
  if (parameters.empty())
    return Error(ErrorCode::kSigningFailed, "empty signed query");
  std::string query;
  for (const auto& [name, value] : parameters) {
    if (name.empty())
      return Error(ErrorCode::kSigningFailed, "empty query name");
    if (!query.empty()) query.push_back('&');
    query += PercentEncode(name);
    query.push_back('=');
    query += PercentEncode(value);
  }
  return query;
}

absl::StatusOr<std::string> HmacSha256Hex(std::string_view secret,
                                          std::string_view payload) {
  if (secret.empty() || secret.size() > INT_MAX || payload.size() > INT_MAX) {
    return Error(ErrorCode::kSigningFailed, "invalid HMAC input");
  }
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int length = 0;
  if (HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
           reinterpret_cast<const unsigned char*>(payload.data()),
           payload.size(), digest.data(), &length) == nullptr ||
      length != 32) {
    return Error(ErrorCode::kSigningFailed, "HMAC-SHA256 failed");
  }
  constexpr char kHex[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(64);
  for (unsigned int index = 0; index < length; ++index) {
    hex.push_back(kHex[digest[index] >> 4]);
    hex.push_back(kHex[digest[index] & 15]);
  }
  return hex;
}

absl::StatusOr<std::string> SignedQuery(const QueryParameters& parameters,
                                        std::string_view secret) {
  auto query = CanonicalQuery(parameters);
  if (!query.ok()) return query.status();
  auto signature = HmacSha256Hex(secret, *query);
  if (!signature.ok()) return signature.status();
  return *query + "&signature=" + *signature;
}

}  // namespace hquant::binance_spot

namespace hquant::binance_spot {
namespace {

bool AtLeast(const Decimal& value, const Decimal& minimum) {
  auto compared = value.Compare(minimum);
  return compared.ok() && *compared >= 0;
}

absl::StatusOr<ExchangeOrderStatus> ParseStatus(std::string_view text) {
  if (text == "NEW") return ExchangeOrderStatus::Open;
  if (text == "PARTIALLY_FILLED") return ExchangeOrderStatus::PartiallyTraded;
  if (text == "FILLED") return ExchangeOrderStatus::Traded;
  if (text == "CANCELED") return ExchangeOrderStatus::Canceled;
  if (text == "REJECTED") return ExchangeOrderStatus::Rejected;
  if (text == "EXPIRED") return ExchangeOrderStatus::Expired;
  return Error(ErrorCode::kOrderReportInvalid, "unknown Binance order status");
}

std::string Milliseconds(UtcTime now) {
  return std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                            now.time_since_epoch())
                            .count());
}

}  // namespace

BinanceOrderGateway::BinanceOrderGateway(boost::asio::io_context& io,
                                         HttpTransport& transport,
                                         const Clock& clock,
                                         BinanceGatewayConfig config,
                                         EventHandler on_event)
    : io_(io),
      transport_(transport),
      clock_(clock),
      config_(std::move(config)),
      on_event_(std::move(on_event)) {}

absl::Status BinanceOrderGateway::ValidateAndQuantize(
    SubmitOrder* request, const StrategyId& strategy_id,
    MonoTime expires_at_mono) const {
  if (!config_.run.IsValid() || !config_.shard.IsValid() ||
      config_.api_key.empty() || config_.secret_key.empty() ||
      config_.recv_window_ms == 0 || config_.recv_window_ms > 60'000 ||
      config_.request_timeout <= std::chrono::milliseconds::zero() ||
      config_.max_pending_submits == 0 || config_.max_pending_cancels == 0 ||
      config_.trading_rule.market != config_.market) {
    return Error(ErrorCode::kGatewayConfigInvalid,
                 "invalid Binance gateway config");
  }
  if (!strategy_id.IsValid())
    return Error(ErrorCode::kOrderStrategyIdInvalid, "invalid strategy ID");
  if (!request->price.IsStrictlyPositive() ||
      !request->quantity.IsStrictlyPositive())
    return Error(ErrorCode::kOrderPriceOrAmountInvalid,
                 "limit price and amount must be positive");
  if (clock_.MonoNow() >= expires_at_mono)
    return Error(ErrorCode::kOrderExpiredBeforeSend,
                 "approved Binance order expired before preparation");
  auto amount = request->quantity.Quantize(config_.trading_rule.base_increment,
                                           RoundingMode::Down);
  auto price = request->price.Quantize(config_.trading_rule.price_increment,
                                       RoundingMode::Down);
  if (!amount.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Binance quantity cannot be quantized to trading rule");
  if (!price.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Binance price cannot be quantized to trading rule");
  if (!price->IsStrictlyPositive())
    return Error(ErrorCode::kOrderPriceOrAmountInvalid,
                 "price is below one price increment");
  if (!amount->IsStrictlyPositive() ||
      !AtLeast(*amount, config_.trading_rule.min_base_amount)) {
    return Error(ErrorCode::kOrderBelowMinAmount,
                 "Binance quantity below minimum amount");
  }
  auto order_value = amount->Multiply(*price);
  if (!order_value.ok())
    return Error(ErrorCode::kDecimalArithmeticFailed,
                 "Binance order value cannot be calculated");
  if (!AtLeast(*order_value, config_.trading_rule.min_order_value))
    return Error(ErrorCode::kOrderBelowMinOrderValue,
                 "Binance order below minimum order value");
  if (config_.trading_rule.max_base_amount &&
      !AtLeast(*config_.trading_rule.max_base_amount, *amount))
    return Error(ErrorCode::kOrderAboveMaxAmount,
                 "Binance quantity above maximum amount");
  request->quantity = *amount;
  request->price = *price;
  return absl::OkStatus();
}

absl::StatusOr<Order> BinanceOrderGateway::PrepareSubmit(
    const SubmitOrder& request, const StrategyId& strategy_id,
    MonoTime expires_at_mono) {
  if (run_collision_) {
    return Error(ErrorCode::kOrderRecoveryInvalid,
                 "run ID collides with recovered order");
  }
  if (pending_submits_ >= config_.max_pending_submits) {
    return Error(ErrorCode::kOrderSendQueueFull, "Binance submit slots full");
  }
  SubmitOrder normalized = request;
  auto status = ValidateAndQuantize(&normalized, strategy_id, expires_at_mono);
  if (!status.ok()) return status;
  if (next_sequence_ > ((uint32_t{1} << 29) - 1)) {
    return Error(ErrorCode::kSequenceExhausted,
                 "Binance client ID sequence exhausted");
  }
  auto id = EncodeClientOrderId(strategy_id, config_.run, config_.shard,
                                next_sequence_);
  if (!id.ok()) return id.status();
  if (historical_ids_.contains(id->value) ||
      known_orders_.contains(id->value)) {
    return Error(ErrorCode::kOrderDuplicate, "Binance client ID collision");
  }
  Order order{*id,
              strategy_id,
              config_.account,
              config_.market,
              normalized.side,
              normalized.quantity,
              normalized.price,
              normalized.time_in_force};
  known_orders_.emplace(id->value,
                        KnownOrder{order, expires_at_mono, State::Prepared});
  ++next_sequence_;
  ++pending_submits_;
  return order;
}

absl::StatusOr<HttpRequest> BinanceOrderGateway::MakeSubmitRequest(
    const KnownOrder& order) const {
  const auto& request = order.order;
  QueryParameters params{
      {"symbol", request.market.native_symbol},
      {"side", request.side == Side::Buy ? "BUY" : "SELL"},
      {"type", request.time_in_force == TimeInForce::PostOnly ? "LIMIT_MAKER"
                                                              : "LIMIT"}};
  if (request.time_in_force != TimeInForce::PostOnly) {
    const auto tif = request.time_in_force;
    params.emplace_back("timeInForce", tif == TimeInForce::Gtc   ? "GTC"
                                       : tif == TimeInForce::Ioc ? "IOC"
                                                                 : "FOK");
  }
  params.emplace_back("quantity", request.quantity.ToString());
  params.emplace_back("price", request.price.ToString());
  params.emplace_back("newClientOrderId", order.order.client_order_id.value);
  params.emplace_back("newOrderRespType", "RESULT");
  params.emplace_back("recvWindow", std::to_string(config_.recv_window_ms));
  params.emplace_back("timestamp", Milliseconds(clock_.UtcNow()));
  auto query = SignedQuery(params, config_.secret_key);
  if (!query.ok()) return query.status();
  const auto deadline =
      std::min(std::chrono::steady_clock::time_point(
                   order.expires_at_mono.time_since_epoch()),
               std::chrono::steady_clock::now() + config_.request_timeout);
  return HttpRequest{"POST",
                     "/api/v3/order?" + *query,
                     {{"X-MBX-APIKEY", config_.api_key}},
                     "",
                     deadline};
}

absl::StatusOr<HttpRequest> BinanceOrderGateway::MakeCancelRequest(
    const KnownOrder& order) const {
  QueryParameters params{
      {"symbol", order.order.market.native_symbol},
      {"origClientOrderId", order.order.client_order_id.value},
      {"recvWindow", std::to_string(config_.recv_window_ms)},
      {"timestamp", Milliseconds(clock_.UtcNow())}};
  auto query = SignedQuery(params, config_.secret_key);
  if (!query.ok()) return query.status();
  return HttpRequest{
      "DELETE",
      "/api/v3/order?" + *query,
      {{"X-MBX-APIKEY", config_.api_key}},
      "",
      std::chrono::steady_clock::now() + config_.request_timeout};
}

absl::Status BinanceOrderGateway::StartPrepared(
    const ClientOrderId& client_order_id) {
  auto found = known_orders_.find(client_order_id.value);
  if (found == known_orders_.end() || found->second.state != State::Prepared) {
    return Error(ErrorCode::kOrderNotFound, "Binance prepared order absent");
  }
  if (clock_.MonoNow() >= found->second.expires_at_mono) {
    return Error(ErrorCode::kOrderExpiredBeforeSend,
                 "Binance order expired before write");
  }
  auto request = MakeSubmitRequest(found->second);
  if (!request.ok()) return request.status();
  if (config_.rate_limiter) {
    const ErrorCode rate = config_.rate_limiter->TryAcquire(
        config_.order_rate_key, RatePriority::Order, 1,
        RateLimiter::Clock::now());
    if (rate != ErrorCode::kOk)
      return Error(rate, "Binance order rate budget unavailable");
  }
  submits_.push_back(Work{client_order_id, std::move(*request), false,
                          found->second.expires_at_mono});
  found->second.state = State::Queued;
  Pump();
  return absl::OkStatus();
}

absl::Status BinanceOrderGateway::AbortPrepared(
    const ClientOrderId& client_order_id) {
  auto found = known_orders_.find(client_order_id.value);
  if (found == known_orders_.end() || found->second.state != State::Prepared) {
    return Error(ErrorCode::kOrderNotCancelable,
                 "Binance order is not abortable");
  }
  known_orders_.erase(found);
  --pending_submits_;
  return absl::OkStatus();
}

absl::Status BinanceOrderGateway::StartCancel(
    const ClientOrderId& client_order_id) {
  auto found = known_orders_.find(client_order_id.value);
  if (found == known_orders_.end() ||
      (found->second.state != State::Submitted &&
       found->second.state != State::Unknown)) {
    return Error(ErrorCode::kOrderNotCancelable,
                 "Binance order not cancelable");
  }
  if (pending_cancel_ids_.contains(client_order_id.value)) {
    return Error(ErrorCode::kOrderCancelPending,
                 "Binance cancel already pending");
  }
  if (pending_cancels_ >= config_.max_pending_cancels) {
    return Error(ErrorCode::kOrderSendQueueFull, "Binance cancel slots full");
  }
  auto request = MakeCancelRequest(found->second);
  if (!request.ok()) return request.status();
  if (config_.rate_limiter) {
    const ErrorCode rate = config_.rate_limiter->TryAcquire(
        config_.cancel_rate_key, RatePriority::Cancel, 1,
        RateLimiter::Clock::now());
    if (rate != ErrorCode::kOk)
      return Error(rate, "Binance cancel rate budget unavailable");
  }
  cancels_.push_back(Work{client_order_id, std::move(*request), true,
                          clock_.MonoNow() + config_.request_timeout});
  ++pending_cancels_;
  pending_cancel_ids_.insert(client_order_id.value);
  Pump();
  return absl::OkStatus();
}

absl::Status BinanceOrderGateway::ObserveHistoricalClientId(
    const ClientOrderId& client_order_id) {
  if (client_order_id.value.empty())
    return Error(ErrorCode::kClientOrderIdInvalid,
                 "empty historical client ID");
  historical_ids_.insert(client_order_id.value);
  auto decoded = DecodeClientOrderId(client_order_id);
  if (decoded.ok() && decoded->run == config_.run) {
    run_collision_ = true;
    return Error(ErrorCode::kOrderRecoveryInvalid,
                 "run ID collides with recovered order");
  }
  return absl::OkStatus();
}

absl::Status BinanceOrderGateway::RestoreOrder(Order order) {
  if (order.client_order_id.value.empty() || !order.strategy_id.IsValid() ||
      order.account != config_.account || order.market != config_.market ||
      known_orders_.contains(order.client_order_id.value)) {
    return Error(ErrorCode::kOrderRecoveryInvalid,
                 "invalid recovered Binance order");
  }
  auto status = ObserveHistoricalClientId(order.client_order_id);
  if (!status.ok()) return status;
  known_orders_.emplace(order.client_order_id.value,
                        KnownOrder{std::move(order), {}, State::Submitted});
  return absl::OkStatus();
}

absl::StatusOr<OrderUpdate> BinanceOrderGateway::ParseSuccess(
    const KnownOrder& order, const HttpResponse& response) const {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (parser.parse(response.body).get(doc)) {
    return Error(ErrorCode::kOrderReportInvalid,
                 "invalid Binance order response JSON");
  }
  std::string_view client;
  std::string_view status_text;
  std::string_view symbol;
  if (doc["clientOrderId"].get(client) || doc["status"].get(status_text) ||
      doc["symbol"].get(symbol) ||
      client != order.order.client_order_id.value ||
      symbol != order.order.market.native_symbol) {
    return Error(ErrorCode::kExchangeOrderIdConflict,
                 "Binance response identity/status mismatch");
  }
  auto status = ParseStatus(status_text);
  if (!status.ok()) return status.status();
  OrderUpdate update;
  update.account = order.order.account;
  update.market = order.order.market;
  update.client_order_id = order.order.client_order_id;
  update.status = *status;
  update.time = EventTime{{}, clock_.UtcNow(), clock_.MonoNow()};
  auto order_id = doc["orderId"];
  uint64_t numeric_id;
  std::string_view string_id;
  if (!order_id.get(numeric_id)) {
    (void)numeric_id;
  } else if (!order_id.get(string_id) && !string_id.empty()) {
    (void)string_id;
  } else {
    return Error(ErrorCode::kOrderReportInvalid,
                 "Binance response missing order ID");
  }
  return update;
}

void BinanceOrderGateway::Emit(GatewayEvent event) const {
  if (on_event_) on_event_(std::move(event));
}

void BinanceOrderGateway::Pump() {
  if (pumping_) return;
  pumping_ = true;
  boost::asio::co_spawn(
      io_,
      [this]() -> boost::asio::awaitable<void> { co_await ProcessQueue(); },
      boost::asio::detached);
}

boost::asio::awaitable<void> BinanceOrderGateway::ProcessQueue() {
  while (!cancels_.empty() || !submits_.empty()) {
    Work work;
    if (!cancels_.empty()) {
      work = std::move(cancels_.front());
      cancels_.pop_front();
    } else {
      work = std::move(submits_.front());
      submits_.pop_front();
    }
    auto found = known_orders_.find(work.client_order_id.value);
    if (clock_.MonoNow() >= work.expires_at_mono ||
        std::chrono::steady_clock::now() >= work.request.deadline) {
      if (work.cancel) {
        --pending_cancels_;
        pending_cancel_ids_.erase(work.client_order_id.value);
        Emit({GatewayEventKind::CancelRejected, work.client_order_id,
              std::nullopt, "cancel expired before write",
              ErrorCode::kOrderExpiredBeforeSend});
      } else {
        --pending_submits_;
        if (found != known_orders_.end()) found->second.state = State::Rejected;
        Emit({GatewayEventKind::BeforeWriteFailed, work.client_order_id,
              std::nullopt, "order expired before write",
              ErrorCode::kOrderExpiredBeforeSend});
      }
      continue;
    }
    if (found == known_orders_.end()) continue;
    if (!work.cancel) found->second.state = State::Sending;
    absl::StatusOr<HttpResponse> response =
        Error(ErrorCode::kOrderSubmissionUnknown,
              "transport did not return a result");
    try {
      response = co_await transport_.Send(std::move(work.request));
    } catch (const std::exception&) {
      response = Error(ErrorCode::kOrderSubmissionUnknown,
                       "transport threw after send attempt");
    }
    if (work.cancel) {
      --pending_cancels_;
      pending_cancel_ids_.erase(work.client_order_id.value);
    } else
      --pending_submits_;
    if (!response.ok() || response->status >= 500) {
      if (!work.cancel) found->second.state = State::Unknown;
      Emit({work.cancel ? GatewayEventKind::CancelUnknown
                        : GatewayEventKind::SubmissionUnknown,
            work.client_order_id, std::nullopt,
            response.ok() ? "Binance 5xx: execution unknown"
                          : "transport result unknown; reconcile original ID",
            ErrorCode::kOrderSubmissionUnknown});
      continue;
    }
    if (response->status == 418 || response->status == 429) {
      if (config_.rate_limiter) {
        config_.rate_limiter->ObserveHttpStatus(
            work.cancel ? config_.cancel_rate_key : config_.order_rate_key,
            response->status, RateLimiter::Clock::now(),
            response->retry_after > std::chrono::steady_clock::duration::zero()
                ? response->retry_after
                : std::chrono::seconds(5));
      }
    }
    if (response->status < 200 || response->status >= 300) {
      if (!work.cancel) found->second.state = State::Rejected;
      std::optional<OrderUpdate> rejected;
      if (!work.cancel) {
        OrderUpdate update;
        update.account = found->second.order.account;
        update.market = found->second.order.market;
        update.client_order_id = work.client_order_id;
        update.status = ExchangeOrderStatus::Rejected;
        update.time = EventTime{{}, clock_.UtcNow(), clock_.MonoNow()};
        rejected = std::move(update);
      }
      const ErrorCode rejection_code =
          response->status == 418   ? ErrorCode::kExchangeIpBanned
          : response->status == 429 ? ErrorCode::kExchangeRateLimited
                                    : ErrorCode::kOrderRejectedByExchange;
      Emit({work.cancel ? GatewayEventKind::CancelRejected
                        : GatewayEventKind::SubmitRejected,
            work.client_order_id, std::move(rejected),
            "Binance HTTP rejection " + std::to_string(response->status),
            rejection_code});
      continue;
    }
    auto update = ParseSuccess(found->second, *response);
    if (!update.ok()) {
      if (!work.cancel) found->second.state = State::Unknown;
      Emit({work.cancel ? GatewayEventKind::CancelUnknown
                        : GatewayEventKind::SubmissionUnknown,
            work.client_order_id, std::nullopt,
            "unverifiable Binance response; reconcile original ID",
            ErrorCode::kOrderSubmissionUnknown});
      continue;
    }
    if (work.cancel && update->status != ExchangeOrderStatus::Canceled) {
      Emit({GatewayEventKind::CancelUnknown, work.client_order_id, std::nullopt,
            "Binance cancel response is not canceled; reconcile original ID",
            ErrorCode::kOrderSubmissionUnknown});
      continue;
    }
    if (work.cancel || update->status == ExchangeOrderStatus::Traded ||
        update->status == ExchangeOrderStatus::Canceled ||
        update->status == ExchangeOrderStatus::Expired ||
        update->status == ExchangeOrderStatus::Rejected) {
      found->second.state = State::Terminal;
    } else {
      found->second.state = State::Submitted;
    }
    Emit({work.cancel ? GatewayEventKind::CancelConfirmed
                      : GatewayEventKind::SubmitConfirmed,
          work.client_order_id, std::move(*update), ""});
  }
  pumping_ = false;
  co_return;
}

}  // namespace hquant::binance_spot
