#include "hbot/cli/cli.h"

#include <array>
#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <variant>

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
      "start", "--config", "paper.yaml", "--state-dir", "/tmp/hbot"};
  auto start = hbot::ParseCliArguments(start_args);
  Require(start.ok() && start->verb == hbot::CliVerb::Start &&
              start->config_path == "paper.yaml",
          "parse start");
  Require(hbot::ControlSocketPath(start->state_dir) == "/tmp/hbot/control.sock",
          "control socket path");
  Require(!hbot::MakeControlRequest(*start, 1).ok(),
          "start has no control request");

  const std::array<std::string_view, 7> history_args{
      "history", "--state-dir", "/tmp/hbot", "--limit",
      "100",     "--cursor",    "next"};
  auto history = hbot::ParseCliArguments(history_args);
  Require(history.ok() && history->history_limit == 100 &&
              history->history_cursor == "next",
          "parse history");
  auto request = hbot::MakeControlRequest(*history, 17);
  Require(request.ok() && request->request_id == 17 &&
              std::holds_alternative<hbot::HistoryRequest>(request->payload),
          "history request");
  const auto& payload = std::get<hbot::HistoryRequest>(request->payload);
  Require(payload.limit == 100 && payload.cursor == "next",
          "history pagination");
  hbot::ControlResponse history_response;
  history_response.request_id = 17;
  history_response.payload =
      hbot::HistoryResponse{"{\"orders\":[],\"next\":null}"};
  auto formatted = hbot::FormatControlResponse(*history, history_response, 17);
  Require(formatted.ok() && formatted->find("orders") != std::string::npos,
          "history response");
  Require(!hbot::FormatControlResponse(*history, history_response, 18).ok(),
          "request ID mismatch");

  const std::array<std::string_view, 3> stop_args{"stop", "--state-dir",
                                                  "/tmp/hbot"};
  auto stop = hbot::ParseCliArguments(stop_args);
  Require(stop.ok() && std::holds_alternative<hbot::StopRequest>(
                           hbot::MakeControlRequest(*stop, 18)->payload),
          "stop request");
  hbot::ControlResponse stop_response;
  stop_response.request_id = 18;
  stop_response.payload = hbot::StopResponse{true};
  Require(hbot::FormatControlResponse(*stop, stop_response, 18).ok(),
          "stop accepted");

  const std::array<std::string_view, 5> invalid_limit{
      "history", "--state-dir", "/tmp/hbot", "--limit", "0"};
  Require(!hbot::ParseCliArguments(invalid_limit).ok(), "zero history limit");
  const std::array<std::string_view, 5> max_limit{
      "history", "--state-dir", "/tmp/hbot", "--limit", "500"};
  Require(hbot::ParseCliArguments(max_limit).ok(), "max history limit");
  const std::array<std::string_view, 5> over_limit{
      "history", "--state-dir", "/tmp/hbot", "--limit", "501"};
  Require(!hbot::ParseCliArguments(over_limit).ok(),
          "history limit over maximum");
  const std::array<std::string_view, 5> duplicate_option{
      "status", "--state-dir", "/tmp/a", "--state-dir", "/tmp/b"};
  Require(!hbot::ParseCliArguments(duplicate_option).ok(), "duplicate option");
  const std::array<std::string_view, 3> misplaced_option{"status", "--config",
                                                         "paper.yaml"};
  Require(!hbot::ParseCliArguments(misplaced_option).ok(), "misplaced config");
  return 0;
}
