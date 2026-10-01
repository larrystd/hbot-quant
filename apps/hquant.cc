#include <cstdint>
#include <iostream>
#include <string_view>
#include <vector>

#include "application/launcher.h"
#include "cli/cli.h"

int main(int argc, char** argv) {
  std::vector<std::string_view> arguments;
  for (int index = 1; index < argc; ++index)
    arguments.emplace_back(argv[index]);
  auto options = hquant::ParseCliArguments(arguments);
  if (!options.ok()) {
    std::cerr << options.status() << '\n';
    return 2;
  }
  if (options->verb == hquant::CliVerb::Start) {
    auto config = hquant::PrepareStart(*options);
    if (!config.ok()) {
      std::cerr << config.status() << '\n';
      return 2;
    }
    const auto result = hquant::Launch(*config, options->state_dir);
    if (!result.ok()) {
      std::cerr << result << '\n';
      return 1;
    }
    return 0;
  }
  constexpr uint64_t kRequestId = 1;
  auto request = hquant::MakeControlRequest(*options, kRequestId);
  if (!request.ok()) {
    std::cerr << request.status() << '\n';
    return 2;
  }
  auto response = hquant::SendControlRequest(options->state_dir, *request);
  if (!response.ok()) {
    std::cerr << response.status() << '\n';
    return 1;
  }
  auto output = hquant::FormatControlResponse(*options, *response, kRequestId);
  if (!output.ok()) {
    std::cerr << output.status() << '\n';
    return 1;
  }
  std::cout << *output << '\n';
  return 0;
}
