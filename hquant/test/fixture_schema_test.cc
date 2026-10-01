#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <string>

#include "hquant/test/fixture_loader.h"

int main(int argc, char** argv) {
  const char* test_srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  if (!test_srcdir || !workspace) return 1;
  const std::filesystem::path root =
      std::filesystem::path(test_srcdir) / workspace / "hquant/test/fixtures";
  std::map<std::string, size_t> minimums{
      {"order_book", 6}, {"order_tracker", 7}, {"simple_pmm", 5}};
  if (argc > 1 && std::string(argv[1]) == "--simulated_exchange")
    minimums["simulated_exchange"] = 6;
  std::set<std::string> case_ids;
  for (const auto& [family, minimum] : minimums) {
    size_t count = 0;
    const auto directory = root / family;
    if (!std::filesystem::exists(directory)) {
      std::cerr << "missing fixture directory: " << directory << '\n';
      return 2;
    }
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
      if (!entry.is_regular_file() || entry.path().extension() != ".json")
        continue;
      auto fixture = hquant::fixtures::LoadFixture(entry.path().string());
      if (!fixture.ok()) {
        std::cerr << fixture.status() << '\n';
        return 3;
      }
      if (fixture->family != family ||
          !case_ids.insert(fixture->case_id).second) {
        std::cerr << "family mismatch or duplicate case id: " << entry.path()
                  << '\n';
        return 4;
      }
      ++count;
    }
    if (count < minimum) {
      std::cerr << family << " needs at least " << minimum << " cases, found "
                << count << '\n';
      return 5;
    }
  }
  return 0;
}
