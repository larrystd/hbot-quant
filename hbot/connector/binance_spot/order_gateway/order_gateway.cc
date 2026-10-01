#include "hbot/connector/binance_spot/order_gateway/order_gateway.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "hbot/connector/binance_spot/order_gateway/client_id_codec.h"
#include "hbot/connector/binance_spot/order_gateway/signer.h"
#include "simdjson.h"

namespace hbot::binance_spot {
namespace {

bool AtLeast(const Decimal& value, const Decimal& minimum) {
  auto compared = value.Compare(minimum);
  return compared.ok() && *compared >= 0;
}

absl::StatusOr<ExchangeOrderStatus> ParseStatus(std::string_view text) {
  if (text == "NEW") return ExchangeOrderStatus::New;
  if (text == "PARTIALLY_FILLED") return ExchangeOrderStatus::PartiallyFilled;
  if (text == "FILLED") return ExchangeOrderStatus::Filled;
  if (text == "CANCELED") return ExchangeOrderStatus::Canceled;
  if (text == "REJECTED") return ExchangeOrderStatus::Rejected;
  if (text == "EXPIRED") return ExchangeOrderStatus::Expired;
  return absl::InvalidArgumentError("unknown Binance order status");
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
    OrderCommand* command) const {
  auto& request = command->request;
  if (!config_.run.IsValid() || !config_.shard.IsValid() ||
      config_.api_key.empty() || config_.secret_key.empty() ||
      config_.recv_window_ms == 0 || config_.recv_window_ms > 60'000 ||
      config_.request_timeout <= std::chrono::milliseconds::zero() ||
      config_.max_pending_submits == 0 || config_.max_pending_cancels == 0 ||
      !command->owner.IsValid() || request.account != config_.account ||
      request.market != config_.market ||
      config_.trading_rule.market != config_.market || !request.limit_price ||
      !request.limit_price->IsStrictlyPositive() ||
      !request.base_amount.IsStrictlyPositive() ||
      clock_.MonoNow() >= command->expires_at_mono) {
    return absl::InvalidArgumentError(
        "invalid or expired Binance order command");
  }
  if (request.type == OrderType::LimitMaker && request.time_in_force) {
    return absl::InvalidArgumentError("LIMIT_MAKER cannot specify timeInForce");
  }
  auto amount = request.base_amount.Quantize(
      config_.trading_rule.base_increment, RoundingMode::Down);
  auto price = request.limit_price->Quantize(
      config_.trading_rule.price_increment, RoundingMode::Down);
  if (!amount.ok()) return amount.status();
  if (!price.ok()) return price.status();
  if (!amount->IsStrictlyPositive() || !price->IsStrictlyPositive() ||
      !AtLeast(*amount, config_.trading_rule.min_base_amount)) {
    return absl::InvalidArgumentError("Binance quantity below trading rule");
  }
  auto notional = amount->Multiply(*price);
  if (!notional.ok()) return notional.status();
  if (!AtLeast(*notional, config_.trading_rule.min_notional) ||
      (config_.trading_rule.max_base_amount &&
       !AtLeast(*config_.trading_rule.max_base_amount, *amount))) {
    return absl::InvalidArgumentError("Binance notional/quantity outside rule");
  }
  request.base_amount = *amount;
  request.limit_price = *price;
  return absl::OkStatus();
}

absl::StatusOr<OrderIntent> BinanceOrderGateway::PrepareSubmit(
    OrderCommand command) {
  if (run_collision_) {
    return absl::FailedPreconditionError(
        "run ID collides with recovered order");
  }
  if (pending_submits_ >= config_.max_pending_submits) {
    return absl::ResourceExhaustedError("Binance submit slots full");
  }
  auto status = ValidateAndQuantize(&command);
  if (!status.ok()) return status;
  if (next_sequence_ > ((uint32_t{1} << 29) - 1)) {
    return absl::ResourceExhaustedError("Binance client ID sequence exhausted");
  }
  auto id =
      EncodeClientId(command.owner, config_.run, config_.shard, next_sequence_);
  if (!id.ok()) return id.status();
  if (historical_ids_.contains(id->value) ||
      known_orders_.contains(id->value)) {
    return absl::AlreadyExistsError("Binance client ID collision");
  }
  OrderIntent intent;
  intent.client_id = *id;
  intent.owner = command.owner;
  intent.request = command.request;
  intent.created_at_utc = clock_.UtcNow();
  known_orders_.emplace(
      id->value, KnownOrder{intent, command.expires_at_mono, State::Prepared});
  ++next_sequence_;
  ++pending_submits_;
  return intent;
}

absl::StatusOr<HttpRequest> BinanceOrderGateway::MakeSubmitRequest(
    const KnownOrder& order) const {
  const auto& request = order.intent.request;
  QueryParameters params{
      {"symbol", request.market.native_symbol},
      {"side", request.side == Side::Buy ? "BUY" : "SELL"},
      {"type", request.type == OrderType::Limit ? "LIMIT" : "LIMIT_MAKER"}};
  if (request.type == OrderType::Limit) {
    const auto tif = request.time_in_force.value_or(TimeInForce::Gtc);
    params.emplace_back("timeInForce", tif == TimeInForce::Gtc   ? "GTC"
                                       : tif == TimeInForce::Ioc ? "IOC"
                                                                 : "FOK");
  }
  params.emplace_back("quantity", request.base_amount.ToString());
  params.emplace_back("price", request.limit_price->ToString());
  params.emplace_back("newClientOrderId", order.intent.client_id.value);
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
  QueryParameters params{{"symbol", order.intent.request.market.native_symbol},
                         {"origClientOrderId", order.intent.client_id.value},
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
    const ClientOrderId& client_id) {
  auto found = known_orders_.find(client_id.value);
  if (found == known_orders_.end() || found->second.state != State::Prepared) {
    return absl::NotFoundError("Binance prepared order absent");
  }
  if (clock_.MonoNow() >= found->second.expires_at_mono) {
    return absl::DeadlineExceededError("Binance order expired before write");
  }
  auto request = MakeSubmitRequest(found->second);
  if (!request.ok()) return request.status();
  if (config_.rate_limiter &&
      config_.rate_limiter->TryAcquire(
          config_.order_rate_key, RatePriority::Order, 1,
          RateLimiter::Clock::now()) != RateDecision::Granted) {
    return absl::ResourceExhaustedError("Binance order rate lease unavailable");
  }
  submits_.push_back(Work{client_id, std::move(*request), false,
                          found->second.expires_at_mono});
  found->second.state = State::Queued;
  Pump();
  return absl::OkStatus();
}

absl::Status BinanceOrderGateway::AbortPrepared(
    const ClientOrderId& client_id) {
  auto found = known_orders_.find(client_id.value);
  if (found == known_orders_.end() || found->second.state != State::Prepared) {
    return absl::FailedPreconditionError("Binance order is not abortable");
  }
  known_orders_.erase(found);
  --pending_submits_;
  return absl::OkStatus();
}

absl::Status BinanceOrderGateway::StartCancel(const OwnerId& owner,
                                              const ClientOrderId& client_id) {
  auto found = known_orders_.find(client_id.value);
  if (found == known_orders_.end() || found->second.intent.owner != owner ||
      (found->second.state != State::Submitted &&
       found->second.state != State::Unknown)) {
    return absl::FailedPreconditionError("Binance order not cancelable");
  }
  if (pending_cancels_ >= config_.max_pending_cancels) {
    return absl::ResourceExhaustedError("Binance cancel slots full");
  }
  if (pending_cancel_ids_.contains(client_id.value)) {
    return absl::AlreadyExistsError("Binance cancel already pending");
  }
  auto request = MakeCancelRequest(found->second);
  if (!request.ok()) return request.status();
  if (config_.rate_limiter &&
      config_.rate_limiter->TryAcquire(
          config_.cancel_rate_key, RatePriority::Cancel, 1,
          RateLimiter::Clock::now()) != RateDecision::Granted) {
    return absl::ResourceExhaustedError(
        "Binance cancel rate lease unavailable");
  }
  cancels_.push_back(Work{client_id, std::move(*request), true,
                          clock_.MonoNow() + config_.request_timeout});
  ++pending_cancels_;
  pending_cancel_ids_.insert(client_id.value);
  Pump();
  return absl::OkStatus();
}

absl::Status BinanceOrderGateway::ObserveHistoricalClientId(
    const ClientOrderId& client_id) {
  if (client_id.value.empty())
    return absl::InvalidArgumentError("empty historical client ID");
  historical_ids_.insert(client_id.value);
  auto decoded = DecodeClientId(client_id);
  if (decoded.ok() && decoded->run == config_.run) {
    run_collision_ = true;
    return absl::FailedPreconditionError(
        "run ID collides with recovered order");
  }
  return absl::OkStatus();
}

absl::Status BinanceOrderGateway::RestoreOrder(OrderIntent intent) {
  if (intent.client_id.value.empty() || intent.owner.IsValid() == false ||
      intent.request.account != config_.account ||
      intent.request.market != config_.market ||
      known_orders_.contains(intent.client_id.value)) {
    return absl::InvalidArgumentError("invalid recovered Binance order");
  }
  auto status = ObserveHistoricalClientId(intent.client_id);
  if (!status.ok()) return status;
  known_orders_.emplace(intent.client_id.value,
                        KnownOrder{std::move(intent), {}, State::Submitted});
  return absl::OkStatus();
}

absl::StatusOr<OrderUpdate> BinanceOrderGateway::ParseSuccess(
    const KnownOrder& order, const HttpResponse& response) const {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (parser.parse(response.body).get(doc)) {
    return absl::InvalidArgumentError("invalid Binance order response JSON");
  }
  std::string_view client;
  std::string_view status_text;
  std::string_view symbol;
  if (doc["clientOrderId"].get(client) || doc["status"].get(status_text) ||
      doc["symbol"].get(symbol) || client != order.intent.client_id.value ||
      symbol != order.intent.request.market.native_symbol) {
    return absl::FailedPreconditionError(
        "Binance response identity/status mismatch");
  }
  auto status = ParseStatus(status_text);
  if (!status.ok()) return status.status();
  OrderUpdate update;
  update.account = order.intent.request.account;
  update.market = order.intent.request.market;
  update.client_id = order.intent.client_id;
  update.exchange_status = *status;
  update.time = EventTime{{}, clock_.UtcNow(), clock_.MonoNow()};
  auto order_id = doc["orderId"];
  uint64_t numeric_id;
  std::string_view string_id;
  if (!order_id.get(numeric_id)) {
    update.exchange_order_id = ExchangeOrderId(std::to_string(numeric_id));
  } else if (!order_id.get(string_id) && !string_id.empty()) {
    update.exchange_order_id = ExchangeOrderId(std::string(string_id));
  } else {
    return absl::InvalidArgumentError("Binance response missing order ID");
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
    auto found = known_orders_.find(work.client_id.value);
    if (clock_.MonoNow() >= work.expires_at_mono ||
        std::chrono::steady_clock::now() >= work.request.deadline) {
      if (work.cancel) {
        --pending_cancels_;
        pending_cancel_ids_.erase(work.client_id.value);
        Emit({GatewayEventKind::CancelRejected, work.client_id, std::nullopt,
              "cancel expired before write"});
      } else {
        --pending_submits_;
        if (found != known_orders_.end()) found->second.state = State::Rejected;
        Emit({GatewayEventKind::BeforeWriteFailed, work.client_id, std::nullopt,
              "order expired before write"});
      }
      continue;
    }
    if (found == known_orders_.end()) continue;
    if (!work.cancel) found->second.state = State::Sending;
    absl::StatusOr<HttpResponse> response =
        absl::UnavailableError("transport did not return a result");
    try {
      response = co_await transport_.Send(std::move(work.request));
    } catch (const std::exception&) {
      response = absl::UnavailableError("transport threw after send attempt");
    }
    if (work.cancel) {
      --pending_cancels_;
      pending_cancel_ids_.erase(work.client_id.value);
    } else
      --pending_submits_;
    if (!response.ok() || response->status >= 500) {
      if (!work.cancel) found->second.state = State::Unknown;
      Emit({work.cancel ? GatewayEventKind::CancelUnknown
                        : GatewayEventKind::SubmissionUnknown,
            work.client_id, std::nullopt,
            response.ok() ? "Binance 5xx: execution unknown"
                          : "transport result unknown; reconcile original ID"});
      continue;
    }
    if (response->status == 418 || response->status == 429) {
      if (config_.rate_limiter) {
        config_.rate_limiter->ObserveHttpStatus(
            work.cancel ? config_.cancel_rate_key : config_.order_rate_key,
            response->status, RateLimiter::Clock::now(),
            std::chrono::seconds(5));
      }
    }
    if (response->status < 200 || response->status >= 300) {
      if (!work.cancel) found->second.state = State::Rejected;
      std::optional<OrderUpdate> rejected;
      if (!work.cancel) {
        OrderUpdate update;
        update.account = found->second.intent.request.account;
        update.market = found->second.intent.request.market;
        update.client_id = work.client_id;
        update.exchange_status = ExchangeOrderStatus::Rejected;
        update.time = EventTime{{}, clock_.UtcNow(), clock_.MonoNow()};
        rejected = std::move(update);
      }
      Emit({work.cancel ? GatewayEventKind::CancelRejected
                        : GatewayEventKind::SubmitRejected,
            work.client_id, std::move(rejected),
            "Binance HTTP rejection " + std::to_string(response->status)});
      continue;
    }
    auto update = ParseSuccess(found->second, *response);
    if (!update.ok()) {
      if (!work.cancel) found->second.state = State::Unknown;
      Emit({work.cancel ? GatewayEventKind::CancelUnknown
                        : GatewayEventKind::SubmissionUnknown,
            work.client_id, std::nullopt,
            "unverifiable Binance response; reconcile original ID"});
      continue;
    }
    if (work.cancel &&
        update->exchange_status != ExchangeOrderStatus::Canceled) {
      Emit({GatewayEventKind::CancelUnknown, work.client_id, std::nullopt,
            "Binance cancel response is not canceled; reconcile original ID"});
      continue;
    }
    if (work.cancel || update->exchange_status == ExchangeOrderStatus::Filled ||
        update->exchange_status == ExchangeOrderStatus::Canceled ||
        update->exchange_status == ExchangeOrderStatus::Expired ||
        update->exchange_status == ExchangeOrderStatus::Rejected) {
      found->second.state = State::Terminal;
    } else {
      found->second.state = State::Submitted;
    }
    Emit({work.cancel ? GatewayEventKind::CancelConfirmed
                      : GatewayEventKind::SubmitConfirmed,
          work.client_id, std::move(*update), ""});
  }
  pumping_ = false;
  co_return;
}

}  // namespace hbot::binance_spot
