#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "application/config.h"
#include "application/quant_server.h"
#include "gtest/gtest.h"

namespace {

std::filesystem::path RunfilesRoot() {
  const char* srcdir = std::getenv("TEST_SRCDIR");
  const char* workspace = std::getenv("TEST_WORKSPACE");
  if (srcdir && workspace) return std::filesystem::path(srcdir) / workspace;
  return std::filesystem::current_path();
}

TEST(QuantServerStopTest, ConcurrentStopAndWait) {
  const auto root = RunfilesRoot();
  auto config =
      hquant::LoadConfig((root / "examples/simulated_replay.yaml").string());
  ASSERT_TRUE(config.ok()) << config.status();
  config->replay_fixture = (root / "examples/replay_market.json").string();

  // Recreate the waiter 32 times so its first wait races with 1024 stop calls.
  for (int iteration = 0; iteration < 32; ++iteration) {
    std::string directory = "/tmp/hquant_stop_XXXXXX";
    ASSERT_NE(mkdtemp(directory.data()), nullptr);
    auto server = hquant::QuantServer::Create(*config, directory);
    ASSERT_TRUE(server.ok()) << server.status();
    ASSERT_TRUE((*server)->Start().ok());

    std::atomic<bool> go{false};
    absl::Status wait_status;
    std::thread waiter([&] {
      while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
      wait_status = (*server)->Wait();
    });
    std::vector<std::thread> stoppers;
    for (int worker = 0; worker < 8; ++worker) {
      stoppers.emplace_back([&] {
        while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
        for (int call = 0; call < 4; ++call) (*server)->RequestStop();
      });
    }
    go.store(true, std::memory_order_release);
    for (auto& stopper : stoppers) stopper.join();
    waiter.join();
    EXPECT_TRUE(wait_status.ok()) << wait_status;
    std::filesystem::remove_all(directory);
  }
}

}  // namespace
