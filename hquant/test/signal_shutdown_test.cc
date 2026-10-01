#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "sqlite3.h"

namespace {

std::filesystem::path RunfilesRoot() {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  if (srcdir && workspace) return std::filesystem::path(srcdir) / workspace;
  return std::filesystem::current_path();
}

bool CleanManifest(const std::filesystem::path& file) {
  sqlite3* db = nullptr;
  if (sqlite3_open_v2(file.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) !=
      SQLITE_OK) {
    sqlite3_close(db);
    return false;
  }
  sqlite3_stmt* query = nullptr;
  const bool prepared =
      sqlite3_prepare_v2(db,
                         "SELECT clean_stopped_at_us FROM run_manifest LIMIT 1",
                         -1, &query, nullptr) == SQLITE_OK;
  const bool clean = prepared && sqlite3_step(query) == SQLITE_ROW &&
                     sqlite3_column_type(query, 0) == SQLITE_INTEGER;
  sqlite3_finalize(query);
  sqlite3_close(db);
  return clean;
}

TEST(SignalShutdownTest, SigtermAndSigintWriteCleanManifest) {
  const auto root = RunfilesRoot();
  const auto binary = root / "apps/hquant_server";
  ASSERT_TRUE(std::filesystem::exists(binary));
  for (const int signal : {SIGTERM, SIGINT}) {
    std::string directory = "/tmp/hquant_signal_XXXXXX";
    ASSERT_NE(mkdtemp(directory.data()), nullptr);
    const auto socket = std::filesystem::path(directory) / "control.sock";
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
      chdir(root.c_str());
      execl(binary.c_str(), binary.c_str(), "examples/simulated_replay.yaml",
            directory.c_str(), nullptr);
      _exit(127);
    }
    bool started = false;
    for (int attempt = 0; attempt < 500; ++attempt) {
      if (std::filesystem::exists(socket)) {
        started = true;
        break;
      }
      int status = 0;
      if (waitpid(child, &status, WNOHANG) == child) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!started) {
      kill(child, SIGKILL);
      waitpid(child, nullptr, 0);
      FAIL() << "engine did not create control socket";
    }
    ASSERT_EQ(kill(child, signal), 0);
    int status = 0;
    bool exited = false;
    for (int attempt = 0; attempt < 500; ++attempt) {
      if (waitpid(child, &status, WNOHANG) == child) {
        exited = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!exited) {
      kill(child, SIGKILL);
      waitpid(child, &status, 0);
    }
    EXPECT_TRUE(exited);
    EXPECT_TRUE(WIFEXITED(status));
    if (WIFEXITED(status)) EXPECT_EQ(WEXITSTATUS(status), 0);
    EXPECT_FALSE(std::filesystem::exists(socket));
    EXPECT_TRUE(
        CleanManifest(std::filesystem::path(directory) / "history.sqlite"));
    std::filesystem::remove_all(directory);
  }
}

}  // namespace
