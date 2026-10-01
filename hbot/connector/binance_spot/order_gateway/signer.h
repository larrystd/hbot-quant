#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"

namespace hbot::binance_spot {

using QueryParameters = std::vector<std::pair<std::string, std::string>>;

// Percent-encode before signing, then send these exact bytes on the wire.
absl::StatusOr<std::string> CanonicalQuery(const QueryParameters& parameters);
absl::StatusOr<std::string> HmacSha256Hex(std::string_view secret,
                                          std::string_view payload);
absl::StatusOr<std::string> SignedQuery(const QueryParameters& parameters,
                                        std::string_view secret);

}  // namespace hbot::binance_spot
