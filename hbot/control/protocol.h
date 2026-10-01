#pragma once

#include <cstdint>
#include <string>
#include <variant>

namespace hbot {

struct StatusRequest {};
struct HistoryRequest {
  uint32_t limit = 20;
  std::string cursor;
};
struct StopRequest {};
using ControlRequestPayload =
    std::variant<StatusRequest, HistoryRequest, StopRequest>;

struct ControlRequest {
  uint32_t schema_version = 1;
  uint64_t request_id = 0;
  ControlRequestPayload payload;
};

struct StatusResponse {
  std::string json;
};
struct HistoryResponse {
  std::string json;
};
struct StopResponse {
  bool accepted = false;
};
struct ControlError {
  std::string code;
  std::string message;
};
using ControlResponsePayload =
    std::variant<StatusResponse, HistoryResponse, StopResponse, ControlError>;

struct ControlResponse {
  uint32_t schema_version = 1;
  uint64_t request_id = 0;
  ControlResponsePayload payload;
};

}  // namespace hbot
