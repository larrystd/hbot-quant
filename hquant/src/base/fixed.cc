#include "hquant/base/fixed.h"

#include <limits>

#include "absl/status/status.h"

namespace hquant::v1 {

absl::StatusOr<uint64_t> ParseFixed(std::string_view text) {
  if (text.empty()) return absl::InvalidArgumentError("empty decimal");
  const size_t dot = text.find('.');
  if (dot != std::string_view::npos && text.find('.', dot + 1) != std::string_view::npos) {
    return absl::InvalidArgumentError("multiple decimal points");
  }
  const std::string_view whole = text.substr(0, dot);
  std::string_view fraction = dot == std::string_view::npos
                                  ? std::string_view{}
                                  : text.substr(dot + 1);
  if (whole.empty() || (dot != std::string_view::npos && fraction.empty())) {
    return absl::InvalidArgumentError("invalid decimal form");
  }
  while (fraction.size() > 9 && fraction.back() == '0') fraction.remove_suffix(1);
  if (fraction.size() > 9) {
    return absl::InvalidArgumentError("more than nine decimal places");
  }
  uint64_t integer = 0;
  for (char digit : whole) {
    if (digit < '0' || digit > '9') {
      return absl::InvalidArgumentError("invalid decimal digit");
    }
    const uint64_t value = static_cast<uint64_t>(digit - '0');
    if (integer > (std::numeric_limits<uint64_t>::max() - value) / 10) {
      return absl::OutOfRangeError("decimal overflow");
    }
    integer = integer * 10 + value;
  }
  uint64_t fractional = 0;
  for (char digit : fraction) {
    if (digit < '0' || digit > '9') {
      return absl::InvalidArgumentError("invalid decimal digit");
    }
    fractional = fractional * 10 + static_cast<uint64_t>(digit - '0');
  }
  for (size_t i = fraction.size(); i < 9; ++i) fractional *= 10;
  if (integer > (std::numeric_limits<uint64_t>::max() - fractional) / kFixedScale) {
    return absl::OutOfRangeError("decimal overflow");
  }
  return integer * kFixedScale + fractional;
}

absl::StatusOr<uint64_t> CheckedAdd(uint64_t left, uint64_t right) {
  if (left > std::numeric_limits<uint64_t>::max() - right) {
    return absl::OutOfRangeError("fixed addition overflow");
  }
  return left + right;
}

absl::StatusOr<uint64_t> CheckedSubtract(uint64_t left, uint64_t right) {
  if (left < right) return absl::OutOfRangeError("fixed subtraction underflow");
  return left - right;
}

absl::StatusOr<uint64_t> MulDivFloor(uint64_t left, uint64_t right,
                                     uint64_t divisor) {
  if (divisor == 0) return absl::InvalidArgumentError("zero divisor");
  const auto wide = static_cast<unsigned __int128>(left) * right / divisor;
  if (wide > std::numeric_limits<uint64_t>::max()) {
    return absl::OutOfRangeError("fixed multiplication overflow");
  }
  return static_cast<uint64_t>(wide);
}

absl::StatusOr<uint64_t> MulDivCeil(uint64_t left, uint64_t right,
                                    uint64_t divisor) {
  if (divisor == 0) return absl::InvalidArgumentError("zero divisor");
  const auto product = static_cast<unsigned __int128>(left) * right;
  const auto wide = product / divisor + (product % divisor != 0);
  if (wide > std::numeric_limits<uint64_t>::max()) {
    return absl::OutOfRangeError("fixed multiplication overflow");
  }
  return static_cast<uint64_t>(wide);
}

absl::StatusOr<uint64_t> RoundUp(uint64_t value, uint64_t increment) {
  if (increment == 0) return absl::InvalidArgumentError("zero increment");
  const uint64_t remainder = value % increment;
  return remainder == 0 ? absl::StatusOr<uint64_t>(value)
                        : CheckedAdd(value, increment - remainder);
}

}  // namespace hquant::v1
