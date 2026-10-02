#include "order/simulated_exchange.h"

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "base/error.h"
#include "gtest/gtest.h"
#include "hquant/test/fixture_loader.h"
#include "simdjson.h"

namespace hquant {
namespace {

Decimal D(std::string_view text) { return *Decimal::Parse(text); }

std::string S(simdjson::dom::element object, const char* key) {
  std::string_view text;
  if (object[key].get(text)) throw std::runtime_error(key);
  return std::string(text);
}

Side ParseSide(simdjson::dom::element object) {
  return S(object, "side") == "Buy" ? Side::Buy : Side::Sell;
}

class FakeClock final : public Clock {
 public:
  void Set(int64_t micros) { micros_ = micros; }
  UtcTime UtcNow() const override {
    return UtcTime(std::chrono::microseconds(micros_));
  }
  MonoTime MonoNow() const override {
    return MonoTime(std::chrono::microseconds(micros_));
  }

 private:
  int64_t micros_ = 0;
};

void ExpectDecimal(const Decimal& actual, std::string_view expected) {
  auto comparison = actual.Compare(D(expected));
  ASSERT_TRUE(comparison.ok()) << comparison.status();
  EXPECT_EQ(*comparison, 0);
}

struct NormalizedEvent {
  std::string kind;
  std::string client_order_id;
  std::string trade_id;
  Decimal price;
  Decimal amount;
  std::string fee_asset;
  Decimal fee;
};

std::vector<NormalizedEvent> Normalize(std::vector<AccountEvent> events) {
  std::vector<NormalizedEvent> result;
  for (const auto& raw : events) {
    if (const auto* trade = std::get_if<TradeUpdate>(&raw)) {
      NormalizedEvent event;
      event.kind = "OrderTraded";
      event.client_order_id = trade->client_order_id->value;
      event.trade_id = trade->exchange_trade_id.value;
      event.price = trade->price;
      event.amount = trade->quantity;
      if (!trade->fees.empty()) {
        event.fee_asset = trade->fees.front().asset.value;
        event.fee = trade->fees.front().signed_amount;
      }
      result.push_back(std::move(event));
    } else if (const auto* update = std::get_if<OrderUpdate>(&raw)) {
      NormalizedEvent event;
      event.client_order_id = update->client_order_id->value;
      switch (update->exchange_status) {
        case ExchangeOrderStatus::Open:
          event.kind = "OrderOpened";
          break;
        case ExchangeOrderStatus::Traded:
          event.kind = "OrderFullyTraded";
          break;
        case ExchangeOrderStatus::Canceled:
          event.kind = "OrderCanceled";
          break;
        case ExchangeOrderStatus::Rejected:
          event.kind = "OrderFailed";
          break;
        default:
          continue;
      }
      result.push_back(std::move(event));
    }
  }
  return result;
}

TEST(SimulatedExchangeTest, ReplaysAllSimulatedExchangeCasesStepByStep) {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  ASSERT_NE(srcdir, nullptr);
  ASSERT_NE(workspace, nullptr);
  const auto directory = std::filesystem::path(srcdir) / workspace /
                         "hquant/test/fixtures/simulated_exchange";
  size_t case_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.path().extension() != ".json") continue;
    auto fixture = fixtures::LoadFixture(entry.path().string());
    ASSERT_TRUE(fixture.ok()) << fixture.status();
    ASSERT_EQ(fixture->family, "simulated_exchange");
    SCOPED_TRACE(fixture->case_id);
    ++case_count;

    std::vector<std::string> submit_ids;
    for (const auto& step : fixture->steps) {
      simdjson::dom::parser parser;
      simdjson::dom::element event = parser.parse(step.event_json).value();
      if (S(event, "kind") == "submit")
        submit_ids.push_back(S(event, "client_order_id"));
    }
    size_t next_id = 0;
    simdjson::dom::parser setup_parser;
    simdjson::dom::element setup =
        setup_parser.parse(fixture->setup_json).value();
    SimulatedExchangeConfig config;
    config.account = AccountId(S(setup, "account"));
    config.market = MarketSpec{
        MarketId{ExchangeId("simulated"), InstrumentKind::Spot,
                 S(setup, "market")},
        AssetId(S(setup, "base_asset")), AssetId(S(setup, "quote_asset"))};
    config.trading_rule = TradingRule{config.market.market,
                                      D(S(setup, "price_increment")),
                                      D(S(setup, "base_increment")),
                                      D("0.001"),
                                      D("0"),
                                      {},
                                      1,
                                      {}};
    config.maker_fee_rate = D(S(setup, "maker_fee_rate"));
    config.buy_fee_from_returns = bool(setup["buy_fee_from_returns"]);
    for (auto [asset, raw] : simdjson::dom::object(setup["initial_balances"])) {
      std::string_view balance = raw;
      config.initial_balances.emplace(std::string(asset), D(balance));
    }
    config.make_client_id = [&](Side) {
      return ClientOrderId(submit_ids.at(next_id++));
    };
    FakeClock clock;
    SimpleSimulatedExchange sim_exchange(config, clock);
    StrategyId strategy_id{uint64_t(setup["strategy_id"]),
                           StrategyName("simple_pmm")};

    for (const auto& step : fixture->steps) {
      SCOPED_TRACE(step.stamp.at_us);
      clock.Set(step.stamp.at_us);
      simdjson::dom::parser event_parser, output_parser;
      simdjson::dom::element event =
          event_parser.parse(step.event_json).value();
      simdjson::dom::element expected =
          output_parser.parse(step.output_json).value();
      const auto kind = S(event, "kind");
      if (kind == "submit") {
        OrderRequest request;
        request.account = config.account;
        request.market = config.market.market;
        request.side = ParseSide(event);
        request.type = OrderType::Limit;
        request.quantity = D(S(event, "amount"));
        request.limit_price = D(S(event, "price"));
        request.time_in_force = TimeInForce::Gtc;
        ApprovedOrder approved{strategy_id, request, HoldId{1},
                               ActionBatchId{1},
                               clock.MonoNow() + std::chrono::seconds(1)};
        auto prepared = sim_exchange.PrepareSubmit(std::move(approved));
        ASSERT_TRUE(prepared.ok()) << prepared.status();
        EXPECT_EQ(prepared->client_order_id.value, S(event, "client_order_id"));
        auto started = sim_exchange.StartPrepared(prepared->client_order_id);
        if (fixture->expectation_kind == "intentional_divergence") {
          EXPECT_EQ(CodeOf(started), ErrorCode::kSimulatedBalanceInsufficient);
        } else
          EXPECT_TRUE(started.ok()) << started;
      } else if (kind == "cancel") {
        EXPECT_TRUE(
            sim_exchange
                .StartCancel(strategy_id, ClientOrderId(S(event, "client_order_id")))
                .ok());
      } else if (kind == "book_bbo") {
        EXPECT_TRUE(
            sim_exchange.OnBookBbo(D(S(event, "bid")), D(S(event, "ask")))
                .ok());
      } else if (kind == "public_trade") {
        EXPECT_TRUE(sim_exchange
                        .OnPublicTrade(ParseSide(event), D(S(event, "price")),
                                       D(S(event, "amount")))
                        .ok());
      } else
        FAIL() << "unknown event " << kind;

      const auto actual_events = Normalize(sim_exchange.DrainEvents());
      auto expected_events = simdjson::dom::array(expected["events"]);
      ASSERT_EQ(actual_events.size(), expected_events.size());
      size_t i = 0;
      for (auto wanted : expected_events) {
        const auto& actual = actual_events[i++];
        EXPECT_EQ(actual.kind, S(wanted, "kind"));
        EXPECT_EQ(actual.client_order_id, S(wanted, "client_order_id"));
        if (actual.kind == "OrderTraded") {
          EXPECT_EQ(actual.trade_id, S(wanted, "trade_id"));
          ExpectDecimal(actual.price, S(wanted, "price"));
          ExpectDecimal(actual.amount, S(wanted, "amount"));
          auto expected_fees = simdjson::dom::object(wanted["fee_by_asset"]);
          ASSERT_EQ(expected_fees.size(), 1);
          for (auto [asset, raw] : expected_fees) {
            std::string_view fee = raw;
            EXPECT_EQ(actual.fee_asset, asset);
            ExpectDecimal(actual.fee, fee);
          }
        }
      }

      const auto actual_orders = sim_exchange.OpenOrders();
      auto expected_orders = simdjson::dom::array(expected["open_orders"]);
      ASSERT_EQ(actual_orders.size(), expected_orders.size());
      i = 0;
      for (auto wanted : expected_orders) {
        const auto& actual = actual_orders[i++];
        EXPECT_EQ(actual.client_order_id.value, S(wanted, "client_order_id"));
        EXPECT_EQ(actual.request.side, ParseSide(wanted));
        ASSERT_TRUE(actual.request.limit_price);
        ExpectDecimal(*actual.request.limit_price, S(wanted, "price"));
        ExpectDecimal(actual.request.quantity, S(wanted, "amount"));
      }
      for (auto [asset, raw] : simdjson::dom::object(expected["balances"])) {
        std::string_view balance = raw;
        ExpectDecimal(sim_exchange.BalanceOf(AssetId(std::string(asset))),
                      balance);
      }
      for (auto [asset, raw] :
           simdjson::dom::object(expected["available_balances"])) {
        std::string_view available = raw;
        ExpectDecimal(
            sim_exchange.AvailableBalance(AssetId(std::string(asset))),
            available);
      }
      for (const auto& asset :
           {config.market.base_asset, config.market.quote_asset}) {
        auto expected_fees = simdjson::dom::object(expected["fees_by_asset"]);
        std::string_view fee;
        if (expected_fees[asset.value].get(fee)) fee = "0";
        ExpectDecimal(sim_exchange.FeesPaid(asset), fee);
      }
    }
  }
  EXPECT_GE(case_count, 7);
}

TEST(SimulatedExchangeTest, AbortedPreparationNeverCreatesAnOrder) {
  FakeClock clock;
  SimulatedExchangeConfig config;
  config.account = AccountId("simulated");
  config.market = MarketSpec{
      MarketId{ExchangeId("simulated"), InstrumentKind::Spot, "BTC-USDT"},
      AssetId("BTC"), AssetId("USDT")};
  config.trading_rule = TradingRule{config.market.market,
                                    D("0.01"),
                                    D("0.001"),
                                    D("0.001"),
                                    D("0"),
                                    {},
                                    1,
                                    {}};
  config.initial_balances = {{"BTC", D("1")}, {"USDT", D("100")}};
  config.make_client_id = [](Side) { return ClientOrderId("B1"); };
  SimpleSimulatedExchange sim_exchange(std::move(config), clock);
  StrategyId strategy_id{1, StrategyName("simple_pmm")};
  OrderRequest request;
  request.account = AccountId("simulated");
  request.market =
      MarketId{ExchangeId("simulated"), InstrumentKind::Spot, "BTC-USDT"};
  request.side = Side::Buy;
  request.quantity = D("0.01");
  request.limit_price = D("100");
  auto prepared = sim_exchange.PrepareSubmit(
      ApprovedOrder{strategy_id, request, {}, {}, {}});
  ASSERT_TRUE(prepared.ok()) << prepared.status();
  EXPECT_TRUE(sim_exchange.DrainEvents().empty());
  EXPECT_TRUE(sim_exchange.OpenOrders().empty());
  EXPECT_TRUE(sim_exchange.AbortPrepared(prepared->client_order_id).ok());
  EXPECT_EQ(CodeOf(sim_exchange.StartPrepared(prepared->client_order_id)),
            ErrorCode::kOrderNotFound);
  EXPECT_TRUE(sim_exchange.DrainEvents().empty());
  ExpectDecimal(sim_exchange.AvailableBalance(AssetId("USDT")), "100");
}

}  // namespace
}  // namespace hquant
