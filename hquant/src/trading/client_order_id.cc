#include "hquant/trading/client_order_id.h"

#include <array>
#include <cstddef>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "openssl/rand.h"

namespace hquant::v1 {

absl::StatusOr<ClientOrderId> NewClientOrderId() {
  std::array<unsigned char, 16> random{};
  if (RAND_bytes(random.data(), static_cast<int>(random.size())) != 1) {
    return absl::UnavailableError("client order ID random source unavailable");
  }

  constexpr char kHex[] = "0123456789abcdef";
  std::string value(34, '0');
  value[0] = 'h';
  value[1] = 'q';
  for (size_t i = 0; i < random.size(); ++i) {
    value[2 + i * 2] = kHex[random[i] >> 4];
    value[3 + i * 2] = kHex[random[i] & 0x0f];
  }
  return ClientOrderId{std::move(value)};
}

}  // namespace hquant::v1
