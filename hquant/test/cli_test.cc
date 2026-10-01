#include "cli/cli.h"

#include <array>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <variant>

#include "base/error.h"

namespace {

void Require(bool condition, const char* label) {
  if (!condition) {
    std::cerr << label << '\n';
    std::exit(1);
  }
}

}  // namespace

int main() {
  const std::array<std::string_view, 5> start_args{
      "start", "--config", "simulated.yaml", "--state-dir", "/tmp/hquant"};
  auto start = hquant::ParseCliArguments(start_args);
  Require(start.ok() && start->verb == hquant::CliVerb::Start &&
              start->config_path == "simulated.yaml",
          "parse start");
  Require(
      hquant::ControlSocketPath(start->state_dir) == "/tmp/hquant/control.sock",
      "control socket path");
  Require(!hquant::MakeControlRequest(*start, 1).ok(),
          "start has no control request");

  const std::array<std::string_view, 7> history_args{
      "history", "--state-dir", "/tmp/hquant", "--limit",
      "100",     "--cursor",    "next"};
  auto history = hquant::ParseCliArguments(history_args);
  Require(history.ok() && history->history_limit == 100 &&
              history->history_cursor == "next",
          "parse history");
  auto request = hquant::MakeControlRequest(*history, 17);
  Require(request.ok() && request->request_id == 17 &&
              std::holds_alternative<hquant::HistoryRequest>(request->payload),
          "history request");
  const auto& payload = std::get<hquant::HistoryRequest>(request->payload);
  Require(payload.limit == 100 && payload.cursor == "next",
          "history pagination");
  hquant::ControlResponse history_response;
  history_response.request_id = 17;
  history_response.payload =
      hquant::HistoryResponse{"{\"orders\":[],\"next\":null}"};
  auto formatted =
      hquant::FormatControlResponse(*history, history_response, 17);
  Require(formatted.ok() && formatted->find("orders") != std::string::npos,
          "history response");
  Require(!hquant::FormatControlResponse(*history, history_response, 18).ok(),
          "request ID mismatch");
  hquant::ControlResponse engine_error;
  engine_error.request_id = 17;
  engine_error.payload = hquant::ControlError{hquant::ErrorCode::kHistoryQueueFull,
                                              "reader queue full"};
  auto command_error =
      hquant::FormatControlResponse(*history, engine_error, 17);
  Require(!command_error.ok() &&
              hquant::CodeOf(command_error.status()) ==
                  hquant::ErrorCode::kCliEngineError &&
              std::string(command_error.status().message())
                      .find("HISTORY_QUEUE_FULL (-17007)") != std::string::npos,
          "engine error name and number");

  const std::array<std::string_view, 3> stop_args{"stop", "--state-dir",
                                                  "/tmp/hquant"};
  auto stop = hquant::ParseCliArguments(stop_args);
  Require(stop.ok() && std::holds_alternative<hquant::StopRequest>(
                           hquant::MakeControlRequest(*stop, 18)->payload),
          "stop request");
  hquant::ControlResponse stop_response;
  stop_response.request_id = 18;
  stop_response.payload = hquant::StopResponse{true};
  Require(hquant::FormatControlResponse(*stop, stop_response, 18).ok(),
          "stop accepted");

  const std::array<std::string_view, 5> invalid_limit{
      "history", "--state-dir", "/tmp/hquant", "--limit", "0"};
  Require(!hquant::ParseCliArguments(invalid_limit).ok(), "zero history limit");
  Require(hquant::CodeOf(hquant::ParseCliArguments(invalid_limit).status()) ==
              hquant::ErrorCode::kCliUsageInvalid,
          "usage code");
  const std::array<std::string_view, 5> max_limit{
      "history", "--state-dir", "/tmp/hquant", "--limit", "500"};
  Require(hquant::ParseCliArguments(max_limit).ok(), "max history limit");
  const std::array<std::string_view, 5> over_limit{
      "history", "--state-dir", "/tmp/hquant", "--limit", "501"};
  Require(!hquant::ParseCliArguments(over_limit).ok(),
          "history limit over maximum");
  const std::array<std::string_view, 5> duplicate_option{
      "status", "--state-dir", "/tmp/a", "--state-dir", "/tmp/b"};
  Require(!hquant::ParseCliArguments(duplicate_option).ok(),
          "duplicate option");
  const std::array<std::string_view, 3> misplaced_option{"status", "--config",
                                                         "simulated.yaml"};
  Require(!hquant::ParseCliArguments(misplaced_option).ok(),
          "misplaced config");
  return 0;
}
