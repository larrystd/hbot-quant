#pragma once

#include <string>

#include "absl/status/status.h"
#include "boost/asio/ssl/context.hpp"

namespace hbot {

struct TlsConfig {
  bool enabled = false;
  bool verify_peer = true;
  std::string ca_file;
  std::string server_name;
};

// Verification is enabled by default. server_name overrides the endpoint host
// for both certificate hostname checking and SNI.
absl::Status ConfigureTlsContext(boost::asio::ssl::context& context,
                                 const TlsConfig& config);

}  // namespace hbot
