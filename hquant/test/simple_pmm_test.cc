#include "strategy/simple_pmm.h"

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "hquant/test/fixture_loader.h"
#include "simdjson.h"

namespace hquant {
namespace {

Decimal D(std::string_view value) { return *Decimal::Parse(value); }

std::string S(simdjson::dom::element object, const char* key) {
  std::string_view text;
  if (object[key].get(text)) throw std::runtime_error(key);
  return std::string(text);
}

int64_t PriceTicksFor(std::string text) {
  const auto point = text.find('.');
  if (point == std::string::npos) return std::stoll(text) * 1000000;
  std::string digits = text.substr(0, point) + text.substr(point + 1);
  const size_t decimals = text.size() - point - 1;
  if (decimals > 6) throw std::runtime_error("test price precision");
  digits.append(6 - decimals, '0');
  return std::stoll(digits);
}

class FakeBook final : public BookView {
 public:
  void SetMid(std::string text) { price_ = PriceTicksFor(std::move(text)); }
  BookSyncState State() const override { return BookSyncState::Live; }
  std::optional<uint64_t> LastSequence() const override { return 1; }
  std::optional<BookLevel> BestBid() const override {
    return BookLevel{PriceTicks{price_}, QuantityLots{1}};
  }
  std::optional<BookLevel> BestAsk() const override { return BestBid(); }
  size_t CopyTopN(Side, std::span<BookLevel>) const override { return 0; }

 private:
  int64_t price_ = 0;
};

class FakeClock final : public Clock {
 public:
  UtcTime UtcNow() const override { return {}; }
  MonoTime MonoNow() const override { return {}; }
};

void CompareDecimal(const Decimal& actual, const std::string& expected) {
  auto comparison = actual.Compare(D(expected));
  ASSERT_TRUE(comparison.ok()) << comparison.status();
  EXPECT_EQ(*comparison, 0);
}

TEST(SimplePmmTest, ReplaysEveryPythonFixtureStep) {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  ASSERT_NE(srcdir, nullptr);
  ASSERT_NE(workspace, nullptr);
  const auto directory = std::filesystem::path(srcdir) / workspace /
                         "hquant/test/fixtures/simple_pmm";
  size_t case_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    if (entry.path().extension() != ".json") continue;
    auto fixture = fixtures::LoadFixture(entry.path().string());
    ASSERT_TRUE(fixture.ok()) << fixture.status();
    ASSERT_EQ(fixture->family, "simple_pmm");
    SCOPED_TRACE(fixture->case_id);
    ++case_count;

    simdjson::dom::parser setup_parser;
    simdjson::dom::element setup =
        setup_parser.parse(fixture->setup_json).value();
    auto config_json = setup["config"].value();
    const auto market_name = S(setup, "market");
    const auto dash = market_name.find('-');
    ASSERT_NE(dash, std::string::npos);
    SimplePmmConfig config;
    config.account = AccountId(S(setup, "account"));
    config.strategy_id =
        StrategyId{uint64_t(setup["strategy_id"]), StrategyName("simple_pmm")};
    config.market = MarketSpec{
        MarketId{ExchangeId("simulated"), InstrumentKind::Spot, market_name},
        AssetId(market_name.substr(0, dash)),
        AssetId(market_name.substr(dash + 1))};
    config.order_amount = D(S(config_json, "order_amount"));
    config.bid_spread = D(S(config_json, "bid_spread"));
    config.ask_spread = D(S(config_json, "ask_spread"));
    config.refresh_interval =
        std::chrono::microseconds(uint64_t(config_json["refresh_interval_us"]));
    config.price_type = S(config_json, "price_type") == "last"
                            ? PmmPriceType::Last
                            : PmmPriceType::Mid;
    SimplePmm strategy(config);
    FakeBook book;
    FakeClock clock;
    BookScale scale{D("0.000001"), D("0.001"), 1};
    TradingRule rule{config.market.market,
                     D("0.01"),
                     D("0.001"),
                     D("0.001"),
                     D("0"),
                     {},
                     1,
                     {}};

    for (const auto& step : fixture->steps) {
      SCOPED_TRACE(step.stamp.at_us);
      simdjson::dom::parser event_parser, output_parser;
      simdjson::dom::element event =
          event_parser.parse(step.event_json).value();
      simdjson::dom::element expected =
          output_parser.parse(step.output_json).value();
      book.SetMid(S(event, "mid_price"));
      std::optional<Decimal> last;
      std::string_view last_text;
      if (!event["last_price"].get(last_text)) last = D(last_text);
      std::vector<OrderSnapshot> orders;
      for (auto raw : simdjson::dom::array(event["active_orders"])) {
        OrderSnapshot order;
        order.client_id = ClientOrderId(S(raw, "client_id"));
        order.strategy_id = config.strategy_id;
        order.request.account = config.account;
        order.request.market = config.market.market;
        order.display_state = OrderDisplayState::Open;
        orders.push_back(std::move(order));
      }
      std::vector<Balance> balances;
      for (auto [asset, raw] :
           simdjson::dom::object(event["available_balances"])) {
        std::string_view amount = raw;
        balances.push_back(Balance{config.account,
                                   AssetId(std::string(asset)),
                                   D(amount),
                                   D(amount),
                                   {}});
      }
      bool ready = event["ready"];
      StrategyInput context{book,   scale,    rule,       last, ready,
                            orders, balances, step.stamp, clock};
      ActionBatch actual = strategy.OnTimer(context);
      auto expected_actions = simdjson::dom::array(expected["actions"]);
      ASSERT_EQ(actual.ordered.size(), expected_actions.size());
      size_t index = 0;
      for (auto wanted : expected_actions) {
        SCOPED_TRACE(index);
        if (S(wanted, "kind") == "cancel") {
          ASSERT_TRUE(
              std::holds_alternative<CancelOrder>(actual.ordered[index]));
          const auto& action = std::get<CancelOrder>(actual.ordered[index]);
          EXPECT_EQ(action.strategy_id, config.strategy_id);
          EXPECT_EQ(action.client_id.value, S(wanted, "client_id"));
        } else {
          ASSERT_TRUE(
              std::holds_alternative<SubmitOrder>(actual.ordered[index]));
          const auto& action = std::get<SubmitOrder>(actual.ordered[index]);
          EXPECT_EQ(action.strategy_id, config.strategy_id);
          EXPECT_EQ(action.request.side,
                    S(wanted, "side") == "Buy" ? Side::Buy : Side::Sell);
          ASSERT_TRUE(action.request.limit_price.has_value());
          CompareDecimal(*action.request.limit_price, S(wanted, "price"));
          CompareDecimal(action.request.base_amount, S(wanted, "amount"));
        }
        ++index;
      }
      auto expected_refresh = expected["next_refresh_at_us"].value();
      if (expected_refresh.is_null())
        EXPECT_FALSE(strategy.NextRefreshAtUs());
      else {
        ASSERT_TRUE(strategy.NextRefreshAtUs());
        EXPECT_EQ(*strategy.NextRefreshAtUs(), uint64_t(expected_refresh));
      }
    }
  }
  EXPECT_GE(case_count, 7);
}

}  // namespace
}  // namespace hquant
