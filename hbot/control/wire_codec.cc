#include "hbot/control/wire_codec.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

#include "absl/status/status.h"
#include "simdjson.h"

namespace hbot {
namespace {

std::string Escape(std::string_view input) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  result.reserve(input.size() + 2);
  result.push_back('"');
  for (unsigned char character : input) {
    switch (character) {
      case '"':
        result += "\\\"";
        break;
      case '\\':
        result += "\\\\";
        break;
      case '\n':
        result += "\\n";
        break;
      case '\r':
        result += "\\r";
        break;
      case '\t':
        result += "\\t";
        break;
      default:
        if (character < 0x20) {
          result += "\\u00";
          result.push_back(hex[character >> 4]);
          result.push_back(hex[character & 15]);
        } else {
          result.push_back(static_cast<char>(character));
        }
    }
  }
  result.push_back('"');
  return result;
}

absl::StatusOr<simdjson::dom::element> Parse(std::string_view json,
                                             simdjson::dom::parser* parser) {
  simdjson::dom::element root;
  if (parser->parse(json).get(root))
    return absl::InvalidArgumentError("invalid control JSON");
  simdjson::dom::object object;
  if (root.get(object))
    return absl::InvalidArgumentError("control frame must be object");
  return root;
}

absl::StatusOr<std::string> JsonObject(std::string_view text) {
  simdjson::dom::parser parser;
  auto root = Parse(text, &parser);
  if (!root.ok()) return root.status();
  return simdjson::minify(*root);
}

absl::StatusOr<std::pair<uint32_t, uint64_t>> Header(
    simdjson::dom::element root) {
  uint64_t version = 0, request_id = 0;
  if (root["schema_version"].get(version) || version != 1 ||
      root["request_id"].get(request_id)) {
    return absl::InvalidArgumentError("unsupported control frame header");
  }
  return std::pair<uint32_t, uint64_t>{1, request_id};
}

}  // namespace

absl::StatusOr<std::string> EncodeControlRequest(
    const ControlRequest& request) {
  if (request.schema_version != 1)
    return absl::InvalidArgumentError("unsupported control version");
  std::string frame = "{\"schema_version\":1,\"request_id\":" +
                      std::to_string(request.request_id);
  if (std::holds_alternative<StatusRequest>(request.payload)) {
    frame += ",\"kind\":\"status\"}";
  } else if (const auto* history =
                 std::get_if<HistoryRequest>(&request.payload)) {
    if (history->limit == 0 || history->limit > 500) {
      return absl::InvalidArgumentError("history limit out of range");
    }
    frame +=
        ",\"kind\":\"history\",\"limit\":" + std::to_string(history->limit) +
        ",\"cursor\":" + Escape(history->cursor) + "}";
  } else {
    frame += ",\"kind\":\"stop\"}";
  }
  return frame;
}

absl::StatusOr<ControlRequest> DecodeControlRequest(std::string_view json) {
  simdjson::dom::parser parser;
  auto root = Parse(json, &parser);
  if (!root.ok()) return root.status();
  auto header = Header(*root);
  if (!header.ok()) return header.status();
  std::string_view kind;
  if ((*root)["kind"].get(kind))
    return absl::InvalidArgumentError("missing request kind");
  ControlRequest request;
  request.request_id = header->second;
  if (kind == "status")
    request.payload = StatusRequest{};
  else if (kind == "stop")
    request.payload = StopRequest{};
  else if (kind == "history") {
    uint64_t limit = 0;
    std::string_view cursor;
    if ((*root)["limit"].get(limit) || limit == 0 || limit > 500 ||
        (*root)["cursor"].get(cursor)) {
      return absl::InvalidArgumentError("invalid history request");
    }
    request.payload =
        HistoryRequest{static_cast<uint32_t>(limit), std::string(cursor)};
  } else
    return absl::InvalidArgumentError("unknown request kind");
  return request;
}

absl::StatusOr<std::string> EncodeControlResponse(
    const ControlResponse& response) {
  if (response.schema_version != 1)
    return absl::InvalidArgumentError("unsupported control version");
  std::string frame = "{\"schema_version\":1,\"request_id\":" +
                      std::to_string(response.request_id);
  if (const auto* status = std::get_if<StatusResponse>(&response.payload)) {
    auto json = JsonObject(status->json);
    if (!json.ok()) return json.status();
    frame += ",\"kind\":\"status\",\"data\":" + *json + "}";
  } else if (const auto* history =
                 std::get_if<HistoryResponse>(&response.payload)) {
    auto json = JsonObject(history->json);
    if (!json.ok()) return json.status();
    frame += ",\"kind\":\"history\",\"data\":" + *json + "}";
  } else if (const auto* stop = std::get_if<StopResponse>(&response.payload)) {
    frame += std::string(",\"kind\":\"stop\",\"accepted\":") +
             (stop->accepted ? "true}" : "false}");
  } else {
    const auto& error = std::get<ControlError>(response.payload);
    frame += ",\"kind\":\"error\",\"code\":" + Escape(error.code) +
             ",\"message\":" + Escape(error.message) + "}";
  }
  return frame;
}

absl::StatusOr<ControlResponse> DecodeControlResponse(std::string_view json) {
  simdjson::dom::parser parser;
  auto root = Parse(json, &parser);
  if (!root.ok()) return root.status();
  auto header = Header(*root);
  if (!header.ok()) return header.status();
  std::string_view kind;
  if ((*root)["kind"].get(kind))
    return absl::InvalidArgumentError("missing response kind");
  ControlResponse response;
  response.request_id = header->second;
  if (kind == "status" || kind == "history") {
    simdjson::dom::element data;
    simdjson::dom::object object;
    if ((*root)["data"].get(data) || data.get(object)) {
      return absl::InvalidArgumentError("invalid response data");
    }
    if (kind == "status")
      response.payload = StatusResponse{simdjson::minify(data)};
    else
      response.payload = HistoryResponse{simdjson::minify(data)};
  } else if (kind == "stop") {
    bool accepted = false;
    if ((*root)["accepted"].get(accepted))
      return absl::InvalidArgumentError("invalid stop response");
    response.payload = StopResponse{accepted};
  } else if (kind == "error") {
    std::string_view code, message;
    if ((*root)["code"].get(code) || (*root)["message"].get(message)) {
      return absl::InvalidArgumentError("invalid error response");
    }
    response.payload = ControlError{std::string(code), std::string(message)};
  } else
    return absl::InvalidArgumentError("unknown response kind");
  return response;
}

}  // namespace hbot
