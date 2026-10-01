#include "application/launcher.h"

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "application/quant_server.h"

namespace hquant {

absl::Status Launch(const AppConfig& config, const std::string& state_dir) {
  auto server = QuantServer::Create(config, state_dir);
  if (!server.ok()) return server.status();
  auto started = (*server)->Start();
  if (!started.ok()) return started;
  return (*server)->Wait();
}

}  // namespace hquant
