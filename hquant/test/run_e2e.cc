#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

int main() {
  const char* test_srcdir = std::getenv("TEST_SRCDIR");
  const char* test_workspace = std::getenv("TEST_WORKSPACE");
  if (!test_srcdir || !test_workspace) {
    std::cerr << "Bazel runfiles environment is missing\n";
    return 2;
  }
  const std::string root = std::string(test_srcdir) + "/" + test_workspace;
  const std::string script = root + "/hquant/test/e2e_replay.py";
  const std::string binary = root + "/hquant/src/hquant_server";
  execlp("python3", "python3", script.c_str(), "--binary", binary.c_str(), "-v",
         static_cast<char*>(nullptr));
  std::perror("cannot start Python end-to-end suite");
  return 2;
}
