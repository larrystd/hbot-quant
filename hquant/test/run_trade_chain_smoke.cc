#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>

int main() {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  if (!srcdir || !workspace) {
    std::cerr << "Bazel test environment is missing\n";
    return 2;
  }
  char path[] = "/tmp/hquant-smoke-XXXXXX";
  if (!mkdtemp(path)) {
    std::perror("cannot create short test state path");
    return 2;
  }
  const std::string root = std::string(srcdir) + "/" + workspace;
  const std::string benchmark = root + "/hquant/bench/trade_chain_bench";
  const std::string server = root + "/hquant/src/hquant_server";
  const std::string state = path;
  const pid_t child = fork();
  if (child == 0) {
    execl(benchmark.c_str(), benchmark.c_str(), "--symbols", "10", "--seconds",
          "3", "--rest-connections", "4", "--server", server.c_str(),
          "--state-dir", state.c_str(), static_cast<char*>(nullptr));
    std::perror("cannot start trade chain smoke test");
    _exit(127);
  }
  if (child < 0) {
    std::perror("cannot fork trade chain smoke test");
    std::filesystem::remove_all(state);
    return 2;
  }
  int status = 0;
  if (waitpid(child, &status, 0) != child) {
    std::perror("cannot wait for trade chain smoke test");
    return 2;
  }
  if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
    std::filesystem::remove_all(state);
    return 0;
  }
  std::cerr << "trade chain smoke failed; state=" << state << '\n';
  return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}
