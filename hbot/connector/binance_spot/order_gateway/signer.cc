#include "hbot/connector/binance_spot/order_gateway/signer.h"

#include <array>
#include <cctype>
#include <climits>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "openssl/evp.h"
#include "openssl/hmac.h"

namespace hbot::binance_spot {
namespace {

std::string PercentEncode(std::string_view text) {
  constexpr char kHex[] = "0123456789ABCDEF";
  std::string output;
  for (unsigned char ch : text) {
    if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
        (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' ||
        ch == '~') {
      output.push_back(static_cast<char>(ch));
    } else {
      output.push_back('%');
      output.push_back(kHex[ch >> 4]);
      output.push_back(kHex[ch & 15]);
    }
  }
  return output;
}

}  // namespace

absl::StatusOr<std::string> CanonicalQuery(const QueryParameters& parameters) {
  if (parameters.empty())
    return absl::InvalidArgumentError("empty signed query");
  std::string query;
  for (const auto& [name, value] : parameters) {
    if (name.empty()) return absl::InvalidArgumentError("empty query name");
    if (!query.empty()) query.push_back('&');
    query += PercentEncode(name);
    query.push_back('=');
    query += PercentEncode(value);
  }
  return query;
}

absl::StatusOr<std::string> HmacSha256Hex(std::string_view secret,
                                          std::string_view payload) {
  if (secret.empty() || secret.size() > INT_MAX || payload.size() > INT_MAX) {
    return absl::InvalidArgumentError("invalid HMAC input");
  }
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int length = 0;
  if (HMAC(EVP_sha256(), secret.data(), static_cast<int>(secret.size()),
           reinterpret_cast<const unsigned char*>(payload.data()),
           payload.size(), digest.data(), &length) == nullptr ||
      length != 32) {
    return absl::InternalError("HMAC-SHA256 failed");
  }
  constexpr char kHex[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(64);
  for (unsigned int index = 0; index < length; ++index) {
    hex.push_back(kHex[digest[index] >> 4]);
    hex.push_back(kHex[digest[index] & 15]);
  }
  return hex;
}

absl::StatusOr<std::string> SignedQuery(const QueryParameters& parameters,
                                        std::string_view secret) {
  auto query = CanonicalQuery(parameters);
  if (!query.ok()) return query.status();
  auto signature = HmacSha256Hex(secret, *query);
  if (!signature.ok()) return signature.status();
  return *query + "&signature=" + *signature;
}

}  // namespace hbot::binance_spot
