#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "boost/asio/awaitable.hpp"

namespace hbot {

struct HttpRequest {
  std::string method;
  std::string target;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
  std::chrono::steady_clock::time_point deadline;
};

struct HttpResponse {
  unsigned status = 0;
  std::string body;
};

class HttpTransport {
 public:
  virtual ~HttpTransport() = default;
  virtual boost::asio::awaitable<absl::StatusOr<HttpResponse>> Send(
      HttpRequest request) = 0;
};

}  // namespace hbot
