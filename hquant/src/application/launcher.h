#pragma once

#include <string>

#include "absl/status/status.h"
#include "application/config.h"

namespace hquant {

// Starts the configured Simulated engine and serves status/history/stop.
absl::Status Launch(const AppConfig& config, const std::string& state_dir);

}  // namespace hquant
