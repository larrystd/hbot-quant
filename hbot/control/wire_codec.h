#pragma once

#include <string>
#include <string_view>

#include "absl/status/statusor.h"
#include "hbot/control/protocol.h"

namespace hbot {

// One compact JSON object per frame. The Unix socket transport supplies frame
// boundaries; no C++ variant layout is sent over the wire.
absl::StatusOr<std::string> EncodeControlRequest(const ControlRequest& request);
absl::StatusOr<ControlRequest> DecodeControlRequest(std::string_view json);
absl::StatusOr<std::string> EncodeControlResponse(
    const ControlResponse& response);
absl::StatusOr<ControlResponse> DecodeControlResponse(std::string_view json);

}  // namespace hbot
