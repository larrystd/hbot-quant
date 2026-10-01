#include "hbot/connector/binance_spot/order_gateway/client_id_codec.h"

#include <cstdint>
#include <string>
#include <string_view>

#include "absl/status/status.h"

namespace hbot::binance_spot {
namespace {
constexpr std::string_view kAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
constexpr uint32_t kMaxShardSequence = (uint32_t{1} << 29) - 1;

void AppendBase32(uint64_t value, int digits, std::string* output) {
  for (int shift = (digits - 1) * 5; shift >= 0; shift -= 5) {
    output->push_back(kAlphabet[(value >> shift) & 31]);
  }
}

absl::StatusOr<uint64_t> ParseBase32(std::string_view text, uint64_t max) {
  uint64_t value = 0;
  for (char ch : text) {
    const auto digit = kAlphabet.find(ch);
    if (digit == std::string_view::npos || value > (max >> 5)) {
      return absl::InvalidArgumentError(
          "invalid client ID base32 digit/overflow");
    }
    value = (value << 5) | digit;
    if (value > max)
      return absl::InvalidArgumentError("client ID field overflow");
  }
  return value;
}
}  // namespace

absl::StatusOr<ClientOrderId> EncodeClientId(const OwnerId& owner, RunId run,
                                             ShardId shard,
                                             uint32_t shard_sequence) {
  if (!owner.IsValid() || !run.IsValid() || !shard.IsValid() ||
      shard_sequence == 0 || shard_sequence > kMaxShardSequence) {
    return absl::InvalidArgumentError("invalid client ID components");
  }
  const uint32_t suffix =
      (static_cast<uint32_t>(shard.value) << 29) | shard_sequence;
  std::string text;
  text.reserve(31);
  text.push_back('H');
  AppendBase32(owner.owner_key, 10, &text);
  AppendBase32(run.value, 13, &text);
  AppendBase32(suffix, 7, &text);
  return ClientOrderId(std::move(text));
}

absl::StatusOr<DecodedClientId> DecodeClientId(const ClientOrderId& id) {
  if (id.value.size() != 31 || id.value[0] != 'H') {
    return absl::InvalidArgumentError("unsupported client ID format");
  }
  auto owner = ParseBase32(std::string_view(id.value).substr(1, 10),
                           (uint64_t{1} << 48) - 1);
  auto run = ParseBase32(std::string_view(id.value).substr(11, 13), UINT64_MAX);
  auto suffix =
      ParseBase32(std::string_view(id.value).substr(24, 7), UINT32_MAX);
  if (!owner.ok()) return owner.status();
  if (!run.ok()) return run.status();
  if (!suffix.ok()) return suffix.status();
  DecodedClientId result{*owner, RunId{*run},
                         ShardId{static_cast<uint8_t>(*suffix >> 29)},
                         static_cast<uint32_t>(*suffix & kMaxShardSequence)};
  if (result.owner_key == 0 || !result.run.IsValid() ||
      result.shard_sequence == 0) {
    return absl::InvalidArgumentError("invalid client ID identity");
  }
  return result;
}

}  // namespace hbot::binance_spot
