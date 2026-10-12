#include "hquant/venue/binance/auth.h"

#include <chrono>
#include <string>

#include "hquant/base/fixed.h"
#include "openssl/evp.h"
#include "openssl/hmac.h"

namespace hquant::v1 {

std::string BinanceHmacSignature(std::string_view secret,
                                 std::string_view payload) {
  unsigned char digest[EVP_MAX_MD_SIZE]{};
  unsigned int length = 0;
  HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
       reinterpret_cast<const unsigned char*>(payload.data()),
       payload.size(), digest, &length);
  constexpr char kHex[] = "0123456789abcdef";
  std::string result;
  result.reserve(length * 2);
  for (unsigned int i = 0; i < length; ++i) {
    result.push_back(kHex[digest[i] >> 4]);
    result.push_back(kHex[digest[i] & 15]);
  }
  return result;
}

std::string BinanceDecimal(uint64_t nanos) {
  const uint64_t whole = nanos / kFixedScale;
  uint64_t fraction = nanos % kFixedScale;
  if (fraction == 0) return std::to_string(whole);
  std::string digits = std::to_string(fraction);
  digits.insert(0, 9 - digits.size(), '0');
  while (digits.back() == '0') digits.pop_back();
  return std::to_string(whole) + '.' + digits;
}

uint64_t BinanceTimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace hquant::v1
