#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "mpdecimal.h"

namespace hbot {

enum class RoundingMode { Down, Up, Floor, Ceiling, HalfEven, HalfUp };

// Immutable 28-significant-digit value. Monetary operations never pass through
// binary floating point. Quantization requires an explicit rounding mode.
class Decimal {
 public:
  Decimal();
  static absl::StatusOr<Decimal> Parse(std::string_view text);

  std::string ToString() const;
  bool IsStrictlyPositive() const;
  bool IsNonnegative() const;
  absl::StatusOr<Decimal> Add(const Decimal& other) const;
  absl::StatusOr<Decimal> Subtract(const Decimal& other) const;
  absl::StatusOr<Decimal> Multiply(const Decimal& other) const;
  absl::StatusOr<Decimal> Divide(const Decimal& other) const;
  absl::StatusOr<Decimal> Quantize(const Decimal& quantum,
                                   RoundingMode rounding) const;
  absl::StatusOr<int> Compare(const Decimal& other) const;

 private:
  struct Deleter {
    void operator()(mpd_t* value) const;
  };
  using Value = std::shared_ptr<mpd_t>;
  explicit Decimal(Value value) : value_(std::move(value)) {}
  static Value NewValue();
  Value value_;
};

}  // namespace hbot
