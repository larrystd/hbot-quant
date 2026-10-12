#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace hquant::v1 {

std::string BinanceHmacSignature(std::string_view secret,
                                 std::string_view payload);
std::string BinanceDecimal(uint64_t nanos);
uint64_t BinanceTimestampMs();

}  // namespace hquant::v1
