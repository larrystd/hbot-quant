#include "hbot/net/tls_config.h"

#include "absl/status/status.h"
#include "boost/asio/ssl.hpp"
#include "boost/system/error_code.hpp"

namespace hbot {

absl::Status ConfigureTlsContext(boost::asio::ssl::context& context,
                                 const TlsConfig& config) {
  if (!config.enabled) return absl::OkStatus();
  boost::system::error_code ec;
  context.set_options(boost::asio::ssl::context::default_workarounds, ec);
  if (ec) return absl::InvalidArgumentError(ec.message());
  if (config.verify_peer) {
    if (config.ca_file.empty())
      context.set_default_verify_paths(ec);
    else
      context.load_verify_file(config.ca_file, ec);
    if (ec) return absl::InvalidArgumentError(ec.message());
    context.set_verify_mode(boost::asio::ssl::verify_peer);
  } else {
    context.set_verify_mode(boost::asio::ssl::verify_none);
  }
  return absl::OkStatus();
}

}  // namespace hbot
