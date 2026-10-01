#include <cstdint>
#include <iostream>
#include <string_view>
#include <vector>

#include "apps/bench_cli.h"
#include "apps/bench_control.h"
#include "apps/bench_feed.h"

int main(int argc, char** argv) {
  std::vector<std::string_view> arguments;
  for (int index = 1; index < argc; ++index)
    arguments.emplace_back(argv[index]);
  if (!arguments.empty() && arguments.front() == "control") {
    auto bench = hquant::ParseControlBenchArguments(
        std::span<const std::string_view>(arguments).subspan(1));
    if (!bench.ok()) {
      std::cerr << bench.status() << '\n';
      return 2;
    }
    auto report = hquant::RunControlBench(*bench);
    if (!report.ok()) {
      std::cerr << report.status() << '\n';
      return 1;
    }
    std::cout << *report << '\n';
    return 0;
  }
  if (!arguments.empty() && arguments.front() == "feed") {
    auto bench = hquant::ParseFeedBenchArguments(
        std::span<const std::string_view>(arguments).subspan(1));
    if (!bench.ok()) {
      std::cerr << bench.status() << '\n';
      return 2;
    }
    auto report = hquant::RunFeedBench(*bench);
    if (!report.ok()) {
      std::cerr << report.status() << '\n';
      return 1;
    }
    std::cout << *report << '\n';
    return 0;
  }
  auto options = hquant::ParseCliArguments(arguments);
  if (!options.ok()) {
    std::cerr << options.status() << '\n';
    return 2;
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
