#pragma once

#include <string>

#include "absl/status/status.h"
#include "hbot/app/config/config.h"

namespace hbot {

// G1 foreground Paper engine. It replays a fixed, versioned market stream,
// then serves status/history/stop from its state directory.
absl::Status RunPaperEngine(const EngineConfig& config,
                            const std::string& state_dir);

}  // namespace hbot
