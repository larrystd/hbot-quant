#include <string>
#include <variant>

#include "hbot/control/wire_codec.h"

int main() {
  hbot::ControlRequest request;
  request.request_id = 42;
  request.payload = hbot::HistoryRequest{20, "abc"};
  auto encoded = hbot::EncodeControlRequest(request);
  if (!encoded.ok()) return 1;
  auto decoded = hbot::DecodeControlRequest(*encoded);
  if (!decoded.ok() || decoded->request_id != 42 ||
      !std::holds_alternative<hbot::HistoryRequest>(decoded->payload) ||
      std::get<hbot::HistoryRequest>(decoded->payload).cursor != "abc")
    return 2;
  hbot::ControlResponse response;
  response.request_id = 42;
  response.payload = hbot::ControlError{"busy", "history \"timeout\"\n"};
  auto reply = hbot::EncodeControlResponse(response);
  if (!reply.ok()) return 3;
  auto parsed = hbot::DecodeControlResponse(*reply);
  if (!parsed.ok() || parsed->request_id != 42 ||
      std::get<hbot::ControlError>(parsed->payload).message !=
          "history \"timeout\"\n")
    return 4;
  if (hbot::DecodeControlRequest(
          "{\"schema_version\":2,\"request_id\":1,\"kind\":\"stop\"}")
          .ok())
    return 5;
  return 0;
}
