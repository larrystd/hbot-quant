#include "order/order_gateway.h"

#include <chrono>
#include <deque>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "base/error.h"
#include "boost/asio.hpp"
#include "gtest/gtest.h"

namespace hquant::binance_spot {
namespace {

Decimal D(const char* text) { return *Decimal::Parse(text); }

class TestClock final : public Clock {
 public:
  UtcTime UtcNow() const override {
    return UtcTime(std::chrono::milliseconds(1'499'827'319'559));
  }
  MonoTime MonoNow() const override {
    return std::chrono::time_point_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now()) +
           offset;
  }
  void Advance(std::chrono::seconds delta) { offset += delta; }

 private:
  std::chrono::microseconds offset{0};
};

std::string Param(const std::string& target, const std::string& name) {
  const std::string needle = name + "=";
  auto first = target.find(needle);
  if (first == std::string::npos) return "";
  first += needle.size();
  const auto last = target.find('&', first);
  return target.substr(first, last == std::string::npos ? last : last - first);
}

class MockTransport final : public HttpTransport {
 public:
  enum class Outcome {
    Success,
    Timeout,
    ServerError,
    Rejected,
    RateLimited,
    Banned,
    WrongIdentity
  };
  std::deque<Outcome> outcomes;
  std::vector<HttpRequest> requests;

  boost::asio::awaitable<absl::StatusOr<HttpResponse>> Send(
      HttpRequest request) override {
    const std::string client = request.method == "DELETE"
                                   ? Param(request.target, "origClientOrderId")
                                   : Param(request.target, "newClientOrderId");
    const bool cancel = request.method == "DELETE";
    requests.push_back(std::move(request));
    const auto outcome = outcomes.front();
    outcomes.pop_front();
    if (outcome == Outcome::Timeout) {
      co_return absl::DeadlineExceededError("mock write timeout");
    }
    if (outcome == Outcome::ServerError) {
      co_return HttpResponse{503, "{}"};
    }
    if (outcome == Outcome::Rejected) {
      co_return HttpResponse{400, R"({"code":-1013,"msg":"filter failure"})"};
    }
    if (outcome == Outcome::RateLimited) {
      co_return HttpResponse{429, R"({"code":-1003,"msg":"rate limit"})"};
    }
    if (outcome == Outcome::Banned) {
      co_return HttpResponse{418, R"({"code":-1003,"msg":"IP banned"})"};
    }
    if (outcome == Outcome::WrongIdentity) {
      co_return HttpResponse{
          200, "{\"symbol\":\"ETHUSDT\",\"clientOrderId\":\"" + client +
                   "\",\"orderId\":123,\"status\":\"NEW\"}"};
    }
    co_return HttpResponse{200, "{\"symbol\":\"BTCUSDT\",\"clientOrderId\":\"" +
                                    client +
                                    "\",\"orderId\":123,\"status\":\"" +
                                    (cancel ? "CANCELED" : "NEW") + "\"}"};
  }
};

BinanceGatewayConfig Config() {
  BinanceGatewayConfig config;
  config.account = AccountId("test-account");
  config.market =
      MarketId{ExchangeId("binance"), InstrumentKind::Spot, "BTCUSDT"};
  config.trading_rule =
      TradingRule{config.market, D("0.01"), D("0.001"), D("0.001"), D("0.01")};
  config.run = RunId{42};
  config.shard = ShardId{3};
  config.api_key = "test-api-key";
  config.secret_key = "test-secret-key";
  config.max_pending_submits = 2;
  config.max_pending_cancels = 1;
  return config;
}

StrategyId MakeStrategyId() {
  return StrategyId{17, StrategyName("simple_pmm")};
}

ApprovedOrder Approved(const TestClock& clock) {
  OrderRequest request;
  request.account = AccountId("test-account");
  request.market = Config().market;
  request.side = Side::Buy;
  request.type = OrderType::LimitMaker;
  request.base_amount = D("0.0109");
  request.limit_price = D("99.999");
  return ApprovedOrder{MakeStrategyId(), request, HoldId{1}, ActionBatchId{1},
                       clock.MonoNow() + std::chrono::seconds(1)};
}

TEST(ClientIdCodecTest, StableStrategyIdAndUniqueRunRoundTrip) {
  auto first = EncodeClientId(MakeStrategyId(), RunId{42}, ShardId{3}, 1);
  auto restarted = EncodeClientId(MakeStrategyId(), RunId{43}, ShardId{7}, 1);
  ASSERT_TRUE(first.ok() && restarted.ok());
  EXPECT_EQ(first->value.size(), 31);
  EXPECT_NE(first->value, restarted->value);
  auto decoded = DecodeClientId(*first);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(decoded->strategy_id, MakeStrategyId().value);
  EXPECT_EQ(decoded->run.value, 42);
  EXPECT_EQ(decoded->shard_hint.value, 3);
  EXPECT_EQ(decoded->shard_sequence, 1);
  EXPECT_EQ(CodeOf(DecodeClientId(ClientOrderId(first->value + "A")).status()),
            ErrorCode::kClientOrderIdInvalid);
  EXPECT_FALSE(EncodeClientId(MakeStrategyId(), RunId{42}, ShardId{3}, 0).ok());
  EXPECT_FALSE(
      EncodeClientId(MakeStrategyId(), RunId{42}, ShardId{3}, 1U << 29).ok());
}

TEST(SignerTest, OfficialHmacVectorAndPercentEncodedWireBytes) {
  const std::string payload =
      "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&"
      "price=0.1&recvWindow=5000&timestamp=1499827319559";
  const std::string secret =
      "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
  auto signature = HmacSha256Hex(secret, payload);
  ASSERT_TRUE(signature.ok());
  EXPECT_EQ(*signature,
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71");
  auto encoded = CanonicalQuery({{"symbol", "BTC/USDT"}, {"note", "a b&c"}});
  ASSERT_TRUE(encoded.ok());
  EXPECT_EQ(*encoded, "symbol=BTC%2FUSDT&note=a%20b%26c");
  auto signed_query = SignedQuery({{"symbol", "BTC/USDT"}}, secret);
  ASSERT_TRUE(signed_query.ok());
  EXPECT_TRUE(signed_query->starts_with("symbol=BTC%2FUSDT&signature="));
}

TEST(BinanceOrderGatewayTest,
     BeforeWriteExpiryReleasesSlotAndEmitsLocalFailure) {
  boost::asio::io_context io;
  MockTransport transport;
  TestClock clock;
  std::vector<GatewayEvent> events;
  auto config = Config();
  config.max_pending_submits = 1;
  BinanceOrderGateway gateway(
      io, transport, clock, config,
      [&](GatewayEvent event) { events.push_back(std::move(event)); });
  auto invalid = Approved(clock);
  invalid.request.base_amount = D("0");
  EXPECT_EQ(CodeOf(gateway.PrepareSubmit(std::move(invalid)).status()),
            ErrorCode::kOrderPriceOrAmountInvalid);
  EXPECT_EQ(gateway.PendingSubmitCount(), 0);
  auto prepared = gateway.PrepareSubmit(Approved(clock));
  ASSERT_TRUE(prepared.ok());
  EXPECT_EQ(prepared->request.base_amount.ToString(), "0.01");
  EXPECT_EQ(prepared->request.limit_price->ToString(), "99.99");
  EXPECT_TRUE(gateway.StartPrepared(prepared->client_id).ok());
  clock.Advance(std::chrono::seconds(2));
  io.run();
  EXPECT_TRUE(transport.requests.empty());
  EXPECT_EQ(gateway.PendingSubmitCount(), 0);
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].kind, GatewayEventKind::BeforeWriteFailed);
  EXPECT_EQ(events[0].code, ErrorCode::kOrderExpiredBeforeSend);
  EXPECT_EQ(events[0].client_id, prepared->client_id);
}

TEST(BinanceOrderGatewayTest, InvalidQuantizationUsesDecimalArithmeticCode) {
  boost::asio::io_context io;
  MockTransport transport;
  TestClock clock;
  auto config = Config();
  config.trading_rule.base_increment = D("0");
  BinanceOrderGateway gateway(io, transport, clock, std::move(config), {});
  EXPECT_EQ(CodeOf(gateway.PrepareSubmit(Approved(clock)).status()),
            ErrorCode::kDecimalArithmeticFailed);
}

TEST(BinanceOrderGatewayTest, TimeoutAndServerErrorRemainUnknownWithoutResend) {
  boost::asio::io_context io;
  MockTransport transport;
  transport.outcomes = {MockTransport::Outcome::Timeout,
                        MockTransport::Outcome::ServerError};
  TestClock clock;
  std::vector<GatewayEvent> events;
  BinanceOrderGateway gateway(
      io, transport, clock, Config(),
      [&](GatewayEvent event) { events.push_back(std::move(event)); });
  auto first = gateway.PrepareSubmit(Approved(clock));
  auto second = gateway.PrepareSubmit(Approved(clock));
  ASSERT_TRUE(first.ok() && second.ok());
  ASSERT_TRUE(gateway.StartPrepared(first->client_id).ok());
  ASSERT_TRUE(gateway.StartPrepared(second->client_id).ok());
  io.run();
  ASSERT_EQ(transport.requests.size(), 2);
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].kind, GatewayEventKind::SubmissionUnknown);
  EXPECT_EQ(events[0].code, ErrorCode::kOrderSubmissionUnknown);
  EXPECT_EQ(events[0].client_id, first->client_id);
  EXPECT_EQ(events[1].kind, GatewayEventKind::SubmissionUnknown);
  EXPECT_EQ(events[1].code, ErrorCode::kOrderSubmissionUnknown);
  EXPECT_EQ(events[1].client_id, second->client_id);
  EXPECT_FALSE(gateway.StartPrepared(first->client_id).ok());
  EXPECT_EQ(transport.requests.size(), 2);
  EXPECT_EQ(Param(transport.requests[0].target, "newClientOrderId"),
            first->client_id.value);
  EXPECT_EQ(Param(transport.requests[1].target, "newClientOrderId"),
            second->client_id.value);
}

TEST(BinanceOrderGatewayTest, DistinguishesRateLimitFromIpBan) {
  boost::asio::io_context io;
  MockTransport transport;
  transport.outcomes = {MockTransport::Outcome::RateLimited,
                        MockTransport::Outcome::Banned};
  TestClock clock;
  std::vector<GatewayEvent> events;
  BinanceOrderGateway gateway(
      io, transport, clock, Config(),
      [&](GatewayEvent event) { events.push_back(std::move(event)); });
  auto first = gateway.PrepareSubmit(Approved(clock));
  auto second = gateway.PrepareSubmit(Approved(clock));
  ASSERT_TRUE(first.ok() && second.ok());
  ASSERT_TRUE(gateway.StartPrepared(first->client_id).ok());
  ASSERT_TRUE(gateway.StartPrepared(second->client_id).ok());
  io.run();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].code, ErrorCode::kExchangeRateLimited);
  EXPECT_EQ(events[1].code, ErrorCode::kExchangeIpBanned);
}

TEST(BinanceOrderGatewayTest, CancelUsesReservedRateSlotAndOriginalClientId) {
  boost::asio::io_context io;
  MockTransport transport;
  transport.outcomes = {MockTransport::Outcome::Success,
                        MockTransport::Outcome::Success};
  TestClock clock;
  std::vector<GatewayEvent> events;
  GlobalRateBreaker breaker;
  RateLimiter limiter(ShardId{3}, &breaker);
  RateLimitKey key{"test-account", "127.0.0.1", "orders"};
  ASSERT_TRUE(
      limiter.Configure(key, RateBudget{2, 1, std::chrono::seconds(1)}));
  auto config = Config();
  config.rate_limiter = &limiter;
  config.order_rate_key = key;
  config.cancel_rate_key = key;
  BinanceOrderGateway gateway(
      io, transport, clock, config,
      [&](GatewayEvent event) { events.push_back(std::move(event)); });
  auto first = gateway.PrepareSubmit(Approved(clock));
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(gateway.StartPrepared(first->client_id).ok());
  io.run();
  ASSERT_EQ(events.size(), 1);
  ASSERT_EQ(events[0].kind, GatewayEventKind::SubmitConfirmed);
  ASSERT_TRUE(events[0].update);
  EXPECT_EQ(events[0].update->exchange_order_id->value, "123");
  auto second = gateway.PrepareSubmit(Approved(clock));
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(CodeOf(gateway.StartPrepared(second->client_id)),
            ErrorCode::kRateBudgetExhausted);
  EXPECT_TRUE(gateway.AbortPrepared(second->client_id).ok());
  EXPECT_TRUE(gateway.StartCancel(MakeStrategyId(), first->client_id).ok());
  EXPECT_EQ(CodeOf(gateway.StartCancel(MakeStrategyId(), first->client_id)),
            ErrorCode::kOrderCancelPending);
  io.restart();
  io.run();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[1].kind, GatewayEventKind::CancelConfirmed);
  EXPECT_EQ(transport.requests[1].method, "DELETE");
  EXPECT_EQ(Param(transport.requests[1].target, "origClientOrderId"),
            first->client_id.value);
}

TEST(BinanceOrderGatewayTest, CancellationRunsAheadOfQueuedSubmissions) {
  boost::asio::io_context io;
  MockTransport transport;
  transport.outcomes = {
      MockTransport::Outcome::Success, MockTransport::Outcome::Success,
      MockTransport::Outcome::Success, MockTransport::Outcome::Success};
  TestClock clock;
  std::vector<GatewayEvent> events;
  BinanceOrderGateway gateway(
      io, transport, clock, Config(),
      [&](GatewayEvent event) { events.push_back(std::move(event)); });
  auto original = gateway.PrepareSubmit(Approved(clock));
  ASSERT_TRUE(original.ok());
  ASSERT_TRUE(gateway.StartPrepared(original->client_id).ok());
  io.run();
  auto a = gateway.PrepareSubmit(Approved(clock));
  auto b = gateway.PrepareSubmit(Approved(clock));
  ASSERT_TRUE(a.ok() && b.ok());
  ASSERT_TRUE(gateway.StartPrepared(a->client_id).ok());
  ASSERT_TRUE(gateway.StartPrepared(b->client_id).ok());
  ASSERT_TRUE(gateway.StartCancel(MakeStrategyId(), original->client_id).ok());
  io.restart();
  io.run();
  ASSERT_EQ(transport.requests.size(), 4);
  EXPECT_EQ(transport.requests[1].method, "DELETE");
  EXPECT_EQ(transport.requests[2].method, "POST");
  EXPECT_EQ(transport.requests[3].method, "POST");
}

TEST(BinanceOrderGatewayTest, HistoricalRunCollisionStopsNewIds) {
  boost::asio::io_context io;
  MockTransport transport;
  TestClock clock;
  BinanceOrderGateway gateway(io, transport, clock, Config(), {});
  auto old = EncodeClientId(MakeStrategyId(), RunId{42}, ShardId{7}, 123);
  ASSERT_TRUE(old.ok());
  EXPECT_EQ(CodeOf(gateway.ObserveHistoricalClientId(*old)),
            ErrorCode::kOrderRecoveryInvalid);
  EXPECT_EQ(CodeOf(gateway.PrepareSubmit(Approved(clock)).status()),
            ErrorCode::kOrderRecoveryInvalid);
}

TEST(BinanceOrderGatewayTest,
     MismatchedSuccessfulResponseRequiresReconciliation) {
  boost::asio::io_context io;
  MockTransport transport;
  transport.outcomes = {MockTransport::Outcome::WrongIdentity};
  TestClock clock;
  std::vector<GatewayEvent> events;
  BinanceOrderGateway gateway(
      io, transport, clock, Config(),
      [&](GatewayEvent event) { events.push_back(std::move(event)); });
  auto prepared = gateway.PrepareSubmit(Approved(clock));
  ASSERT_TRUE(prepared.ok());
  ASSERT_TRUE(gateway.StartPrepared(prepared->client_id).ok());
  io.run();
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].kind, GatewayEventKind::SubmissionUnknown);
  EXPECT_EQ(events[0].code, ErrorCode::kOrderSubmissionUnknown);
  EXPECT_EQ(events[0].client_id, prepared->client_id);
  EXPECT_EQ(transport.requests.size(), 1);
}

}  // namespace
}  // namespace hquant::binance_spot
