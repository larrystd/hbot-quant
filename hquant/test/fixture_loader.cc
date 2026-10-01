#include "hquant/test/fixture_loader.h"

#include <cstdint>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "base/types.h"
#include "simdjson.h"

namespace hquant::fixtures {
namespace {

constexpr std::string_view kBaseline =
    "9af100d6822da7d2d0291a906c730ef172284ee2";

bool String(simdjson::dom::element object, const char* key, std::string* out) {
  std::string_view value;
  if (object[key].get(value)) return false;
  *out = std::string(value);
  return true;
}

bool U64(simdjson::dom::element object, const char* key, uint64_t* out) {
  return !object[key].get(*out);
}

bool Object(simdjson::dom::element object, const char* key,
            simdjson::dom::element* out) {
  if (object[key].get(*out)) return false;
  simdjson::dom::object value;
  return !out->get(value);
}

bool Array(simdjson::dom::element object, const char* key,
           simdjson::dom::array* out) {
  return !object[key].get(*out);
}

bool OneOf(std::string_view value,
           std::initializer_list<std::string_view> allowed) {
  for (auto item : allowed)
    if (value == item) return true;
  return false;
}

bool DecimalString(simdjson::dom::element object, const char* key) {
  std::string text;
  return String(object, key, &text) && Decimal::Parse(text).ok();
}

bool PositiveDecimalString(simdjson::dom::element object, const char* key) {
  std::string text;
  if (!String(object, key, &text)) return false;
  auto decimal = Decimal::Parse(text);
  return decimal.ok() && decimal->IsStrictlyPositive();
}

bool DecimalMap(simdjson::dom::element object, const char* key) {
  simdjson::dom::object map;
  if (object[key].get(map)) return false;
  for (auto [asset, value] : map) {
    std::string_view decimal;
    if (asset.empty() || value.get(decimal) || !Decimal::Parse(decimal).ok())
      return false;
  }
  return true;
}

bool ValidateSetup(const std::string& family, simdjson::dom::element setup) {
  std::string market;
  if (!String(setup, "market", &market) || market.empty()) return false;
  if (family == "order_book") {
    uint64_t scale = 0, epoch = 0;
    return PositiveDecimalString(setup, "quote_per_tick") &&
           PositiveDecimalString(setup, "base_per_lot") &&
           U64(setup, "scale_version", &scale) && scale > 0 &&
           U64(setup, "stream_epoch", &epoch) && epoch > 0;
  }
  std::string account;
  uint64_t strategy_id = 0;
  if (!String(setup, "account", &account) || account.empty() ||
      !U64(setup, "strategy_id", &strategy_id) || strategy_id == 0 ||
      strategy_id >= (uint64_t{1} << 48)) {
    return false;
  }
  if (family == "order_tracker") {
    std::string client_id, side;
    return String(setup, "client_id", &client_id) && !client_id.empty() &&
           String(setup, "side", &side) && OneOf(side, {"Buy", "Sell"}) &&
           PositiveDecimalString(setup, "base_amount") &&
           PositiveDecimalString(setup, "limit_price") &&
           DecimalMap(setup, "initial_reservation_by_asset");
  }
  if (family == "simple_pmm") {
    simdjson::dom::element config;
    uint64_t refresh = 0;
    std::string price_type;
    return Object(setup, "config", &config) &&
           PositiveDecimalString(config, "order_amount") &&
           DecimalString(config, "bid_spread") &&
           DecimalString(config, "ask_spread") &&
           U64(config, "refresh_interval_us", &refresh) && refresh > 0 &&
           String(config, "price_type", &price_type) &&
           OneOf(price_type, {"mid", "last"});
  }
  if (family == "simulated_exchange") {
    std::string base, quote;
    bool buy_fee_from_returns = false;
    return String(setup, "base_asset", &base) && !base.empty() &&
           String(setup, "quote_asset", &quote) && !quote.empty() &&
           DecimalMap(setup, "initial_balances") &&
           DecimalString(setup, "maker_fee_rate") &&
           PositiveDecimalString(setup, "price_increment") &&
           PositiveDecimalString(setup, "base_increment") &&
           !setup["buy_fee_from_returns"].get(buy_fee_from_returns);
  }
  return false;
}

bool Levels(simdjson::dom::element object, const char* key) {
  simdjson::dom::array levels;
  if (!Array(object, key, &levels)) return false;
  for (auto level : levels) {
    simdjson::dom::array pair;
    if (level.get(pair) || pair.size() != 2) return false;
    int64_t price = 0;
    uint64_t quantity = 0;
    if (pair.at(0).get(price) || pair.at(1).get(quantity) || price <= 0)
      return false;
  }
  return true;
}

bool ValidateEvent(const std::string& family, simdjson::dom::element event) {
  std::string kind;
  if (!String(event, "kind", &kind)) return false;
  if (family == "order_book") {
    if (!OneOf(kind, {"subscribe", "snapshot", "diff", "disconnect", "timer",
                      "invalid_raw_level"}))
      return false;
    if (kind == "snapshot" || kind == "diff") {
      uint64_t epoch = 0, last = 0;
      if (!U64(event, "stream_epoch", &epoch) ||
          !U64(event, "last_sequence", &last) || !Levels(event, "bids") ||
          !Levels(event, "asks"))
        return false;
      if (kind == "diff") {
        uint64_t first = 0;
        if (!U64(event, "first_sequence", &first) || first > last) return false;
      }
    }
    if (kind == "invalid_raw_level") {
      std::string side, price, quantity, reason;
      if (!String(event, "side", &side) || !OneOf(side, {"Buy", "Sell"}) ||
          !DecimalString(event, "raw_price") ||
          !DecimalString(event, "raw_quantity") ||
          !String(event, "reason", &reason))
        return false;
    }
    return true;
  }
  if (family == "order_tracker") {
    if (!OneOf(kind, {"register", "order_update", "trade_update",
                      "cancel_requested", "submission_unknown", "reconcile"}))
      return false;
    if (kind == "order_update") {
      std::string status;
      return String(event, "exchange_status", &status);
    }
    if (kind == "trade_update") {
      std::string trade_id, price, amount, quote;
      return String(event, "trade_id", &trade_id) &&
             DecimalString(event, "price") &&
             DecimalString(event, "base_amount") &&
             DecimalString(event, "quote_amount") &&
             DecimalMap(event, "fees_by_asset");
    }
    return true;
  }
  if (family == "simple_pmm") {
    if (kind != "tick") return false;
    bool ready = false;
    std::string mid;
    simdjson::dom::array active;
    return !event["ready"].get(ready) && DecimalString(event, "mid_price") &&
           Array(event, "active_orders", &active) &&
           DecimalMap(event, "available_balances");
  }
  if (family == "simulated_exchange") {
    if (kind == "tick") return true;
    if (kind == "cancel") {
      std::string id;
      return String(event, "client_id", &id) && !id.empty();
    }
    if (kind == "book_bbo") {
      return PositiveDecimalString(event, "bid") &&
             PositiveDecimalString(event, "ask");
    }
    if (kind == "submit" || kind == "public_trade") {
      std::string side;
      if (!String(event, "side", &side) || !OneOf(side, {"Buy", "Sell"}) ||
          !PositiveDecimalString(event, "price") ||
          !PositiveDecimalString(event, "amount"))
        return false;
      if (kind == "submit") {
        std::string id;
        return String(event, "client_id", &id) && !id.empty();
      }
      return true;
    }
    return false;
  }
  return false;
}

bool ValidateOutput(const std::string& family, simdjson::dom::element output) {
  if (family == "order_book") {
    std::string state, reason;
    bool applied = false;
    simdjson::dom::element bbo;
    if (!String(output, "state", &state) ||
        !OneOf(state, {"Subscribing", "Buffering", "Replaying", "Live", "Stale",
                       "Resyncing"}) ||
        !String(output, "reason", &reason) ||
        !OneOf(reason, {"None", "OldDiff", "Gap", "CrossedBook", "InvalidScale",
                        "InvalidMessage", "BufferOverflow", "Disconnected",
                        "Expired"}) ||
        output["applied"].get(applied) || !Object(output, "bbo", &bbo) ||
        !Levels(output, "top_bids") || !Levels(output, "top_asks"))
      return false;
    for (const char* side : {"bid", "ask"}) {
      simdjson::dom::element best;
      if (bbo[side].get(best)) return false;
      if (!best.is_null()) {
        simdjson::dom::array pair;
        if (best.get(pair) || pair.size() != 2) return false;
        int64_t price = 0;
        uint64_t quantity = 0;
        if (pair.at(0).get(price) || pair.at(1).get(quantity) || price <= 0)
          return false;
      }
    }
    simdjson::dom::element sequence;
    if (output["last_sequence"].get(sequence)) return false;
    uint64_t last = 0;
    return sequence.is_null() || !sequence.get(last);
  }
  if (family == "order_tracker") {
    std::string state, base, quote, remaining;
    simdjson::dom::array events;
    if (!String(output, "display_state", &state) ||
        !OneOf(state, {"Absent", "PendingCreate", "Open", "PartiallyTraded",
                       "PendingCancel", "SubmissionUnknown", "AwaitingTrades",
                       "Traded", "Canceled", "Failed", "Expired"}) ||
        !DecimalString(output, "cumulative_base") ||
        !DecimalString(output, "cumulative_quote") ||
        !DecimalString(output, "remaining_base") ||
        !DecimalMap(output, "fees_by_asset") ||
        !Array(output, "events", &events))
      return false;
    for (auto event : events) {
      std::string_view name;
      if (event.get(name) ||
          !OneOf(name, {"OrderCreated", "OrderFilled", "OrderCompleted",
                        "OrderCanceled", "OrderFailed"}))
        return false;
    }
    return true;
  }
  if (family == "simple_pmm") {
    simdjson::dom::array actions;
    simdjson::dom::element refresh;
    if (!Array(output, "actions", &actions) ||
        output["next_refresh_at_us"].get(refresh))
      return false;
    uint64_t next = 0;
    if (!refresh.is_null() && refresh.get(next)) return false;
    for (auto action : actions) {
      std::string kind;
      if (!String(action, "kind", &kind)) return false;
      if (kind == "cancel") {
        std::string id;
        if (!String(action, "client_id", &id)) return false;
      } else if (kind == "submit") {
        std::string side, price, amount;
        if (!String(action, "side", &side) || !OneOf(side, {"Buy", "Sell"}) ||
            !DecimalString(action, "price") || !DecimalString(action, "amount"))
          return false;
      } else
        return false;
    }
    return true;
  }
  if (family == "simulated_exchange") {
    simdjson::dom::array events, orders;
    if (!Array(output, "events", &events) ||
        !Array(output, "open_orders", &orders) ||
        !DecimalMap(output, "balances") ||
        !DecimalMap(output, "available_balances") ||
        !DecimalMap(output, "fees_by_asset"))
      return false;
    for (auto event : events) {
      std::string kind, id;
      if (!String(event, "kind", &kind) ||
          !OneOf(kind, {"OrderCreated", "OrderFilled", "OrderCompleted",
                        "OrderCanceled", "OrderFailed"}) ||
          !String(event, "client_id", &id) || id.empty())
        return false;
      if (kind == "OrderFilled") {
        std::string trade_id;
        if (!String(event, "trade_id", &trade_id) || trade_id.empty() ||
            !PositiveDecimalString(event, "price") ||
            !PositiveDecimalString(event, "amount") ||
            !DecimalMap(event, "fee_by_asset"))
          return false;
      }
    }
    for (auto order : orders) {
      std::string id, side;
      if (!String(order, "client_id", &id) || id.empty() ||
          !String(order, "side", &side) || !OneOf(side, {"Buy", "Sell"}) ||
          !PositiveDecimalString(order, "price") ||
          !PositiveDecimalString(order, "amount"))
        return false;
    }
    return true;
  }
  return false;
}

}  // namespace

absl::StatusOr<FixtureCase> LoadFixture(const std::string& path) {
  std::ifstream file(path);
  if (!file) return absl::NotFoundError("fixture file not found: " + path);
  std::string json(std::istreambuf_iterator<char>{file}, {});
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root))
    return absl::InvalidArgumentError("invalid JSON: " + path);

  FixtureCase result;
  uint64_t version = 0;
  if (!U64(root, "schema_version", &version) || version != 1 ||
      !String(root, "family", &result.family) ||
      !OneOf(result.family,
             {"order_book", "order_tracker", "simple_pmm", "simulated_exchange"}) ||
      !String(root, "case_id", &result.case_id) ||
      result.case_id.rfind(result.family + "/", 0) != 0 ||
      !String(root, "baseline_commit", &result.baseline_commit) ||
      result.baseline_commit != kBaseline ||
      !String(root, "expectation_kind", &result.expectation_kind) ||
      !OneOf(result.expectation_kind, {"parity", "intentional_divergence"})) {
    return absl::InvalidArgumentError("invalid fixture envelope: " + path);
  }
  simdjson::dom::element divergence;
  if (root["divergence_reason"].get(divergence)) {
    return absl::InvalidArgumentError("missing divergence_reason: " + path);
  }
  if (!divergence.is_null()) {
    std::string_view text;
    if (divergence.get(text))
      return absl::InvalidArgumentError("invalid divergence_reason");
    result.divergence_reason = std::string(text);
  }
  if ((result.expectation_kind == "intentional_divergence") !=
      !result.divergence_reason.empty()) {
    return absl::InvalidArgumentError("divergence reason/kind mismatch: " +
                                      path);
  }

  simdjson::dom::array sources;
  if (!Array(root, "source_files", &sources) || sources.size() == 0) {
    return absl::InvalidArgumentError("source_files required: " + path);
  }
  for (auto source : sources) {
    std::string_view name;
    if (source.get(name) || name.empty()) {
      return absl::InvalidArgumentError("invalid source file: " + path);
    }
    result.source_files.emplace_back(name);
  }

  simdjson::dom::element setup;
  simdjson::dom::array inputs, expected;
  if (!Object(root, "setup", &setup) || !ValidateSetup(result.family, setup) ||
      !Array(root, "inputs", &inputs) || !Array(root, "expected", &expected) ||
      inputs.size() == 0 || inputs.size() != expected.size()) {
    return absl::InvalidArgumentError("invalid fixture steps: " + path);
  }
  result.setup_json = simdjson::minify(setup);

  auto expectation = expected.begin();
  InputStamp previous{-1, 0};
  for (auto input : inputs) {
    auto exp = *expectation++;
    uint64_t at = 0, ordinal = 0, expected_at = 0, expected_ordinal = 0;
    simdjson::dom::element event, output;
    if (!U64(input, "at_us", &at) || !U64(input, "ordinal", &ordinal) ||
        !U64(exp, "at_us", &expected_at) ||
        !U64(exp, "ordinal", &expected_ordinal) || at != expected_at ||
        ordinal != expected_ordinal ||
        at > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ||
        !Object(input, "event", &event) || !Object(exp, "output", &output) ||
        !ValidateEvent(result.family, event) ||
        !ValidateOutput(result.family, output) ||
        (previous.at_us >= 0 && (at < static_cast<uint64_t>(previous.at_us) ||
                                 (at == static_cast<uint64_t>(previous.at_us) &&
                                  ordinal <= previous.ordinal)))) {
      return absl::InvalidArgumentError("invalid step: " + path);
    }
    std::string risk_json;
    simdjson::dom::element risk;
    if (!exp["risk_expectation"].get(risk)) {
      simdjson::dom::object risk_object;
      if (risk.get(risk_object) || !DecimalMap(risk, "reservation_by_asset")) {
        return absl::InvalidArgumentError("invalid risk_expectation: " + path);
      }
      risk_json = simdjson::minify(risk);
    }
    result.steps.push_back(FixtureStep{
        InputStamp{static_cast<int64_t>(at), ordinal}, simdjson::minify(event),
        simdjson::minify(output), std::move(risk_json)});
    previous = result.steps.back().stamp;
  }
  return result;
}

}  // namespace hquant::fixtures
