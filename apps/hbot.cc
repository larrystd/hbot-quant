#include <cstdint>
#include <iostream>
#include <string_view>
#include <vector>

#include "hbot/app/engine.h"
#include "hbot/cli/cli.h"

int main(int argc, char** argv) {
  std::vector<std::string_view> arguments;
  for (int index = 1; index < argc; ++index)
    arguments.emplace_back(argv[index]);
  auto options = hbot::ParseCliArguments(arguments);
  if (!options.ok()) {
    std::cerr << options.status() << '\n';
    return 2;
  }
  if (options->verb == hbot::CliVerb::Start) {
    auto config = hbot::PrepareStart(*options);
    if (!config.ok()) {
      std::cerr << config.status() << '\n';
      return 2;
    }
    const auto result = hbot::RunPaperEngine(*config, options->state_dir);
    if (!result.ok()) {
      std::cerr << result << '\n';
      return 1;
    }
    return 0;
  }
  constexpr uint64_t kRequestId = 1;
  auto request = hbot::MakeControlRequest(*options, kRequestId);
  if (!request.ok()) {
    std::cerr << request.status() << '\n';
    return 2;
  }
  auto response = hbot::SendControlRequest(options->state_dir, *request);
  if (!response.ok()) {
    std::cerr << response.status() << '\n';
    return 1;
  }
  auto output = hbot::FormatControlResponse(*options, *response, kRequestId);
  if (!output.ok()) {
    std::cerr << output.status() << '\n';
    return 1;
  }
  std::cout << *output << '\n';
  return 0;
}
