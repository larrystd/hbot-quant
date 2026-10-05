#pragma once

#include <cstdint>
#include <string_view>

#include "absl/status/statusor.h"

namespace hquant::v1 {

// All configuration amounts and ratios use nine decimal places. Market book
// prices and quantities are integer ticks/lots and never use this scale.
inline constexpr uint64_t kFixedScale = 1'000'000'000;

absl::StatusOr<uint64_t> ParseFixed(std::string_view text);
absl::StatusOr<uint64_t> CheckedAdd(uint64_t left, uint64_t right);
absl::StatusOr<uint64_t> CheckedSubtract(uint64_t left, uint64_t right);
absl::StatusOr<uint64_t> MulDivFloor(uint64_t left, uint64_t right,
                                     uint64_t divisor);
absl::StatusOr<uint64_t> MulDivCeil(uint64_t left, uint64_t right,
                                    uint64_t divisor);
absl::StatusOr<uint64_t> RoundUp(uint64_t value, uint64_t increment);

}  // namespace hquant::v1
