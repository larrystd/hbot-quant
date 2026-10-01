#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hbot/base/clock.h"
#include "hbot/base/decimal.h"
#include "simdjson.h"

namespace hbot::binance_spot::private_detail {

inline absl::Status Invalid(std::string_view detail) {
  return absl::InvalidArgumentError(std::string(detail));
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

}  // namespace hbot::binance_spot::private_detail
