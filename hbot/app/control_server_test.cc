#include "hbot/app/control_server.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <variant>

#include "gtest/gtest.h"
#include "hbot/cli/cli.h"

namespace hbot {
namespace {

TEST(ControlServerTest, StopCanPassAnInFlightHistoryRequest) {
  std::string directory =
      (std::filesystem::temp_directory_path() / "hbot_control_test_XXXXXX")
          .string();
  ASSERT_NE(mkdtemp(directory.data()), nullptr);
  std::atomic<bool> history_entered{false};
  auto server = ControlServer::Start(
      ControlSocketPath(directory), [&](const ControlRequest& request) {
        ControlResponse response;
        if (std::holds_alternative<HistoryRequest>(request.payload)) {
          history_entered = true;
          std::this_thread::sleep_for(std::chrono::milliseconds(250));
          response.payload = HistoryResponse{"{\"rows\":[]}"};
        } else
          response.payload = StopResponse{true};
        return response;
      });
  ASSERT_TRUE(server.ok()) << server.status();
  ControlRequest history;
  history.request_id = 1;
  history.payload = HistoryRequest{};
  std::thread pending([&] {
    auto answer = SendControlRequest(directory, history);
    EXPECT_TRUE(answer.ok()) << answer.status();
  });
  for (int attempt = 0; attempt < 100 && !history_entered; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ASSERT_TRUE(history_entered);
  ControlRequest stop;
  stop.request_id = 2;
  stop.payload = StopRequest{};
  const auto started = std::chrono::steady_clock::now();
  auto answer = SendControlRequest(directory, stop);
  const auto elapsed = std::chrono::steady_clock::now() - started;
  ASSERT_TRUE(answer.ok()) << answer.status();
  ASSERT_TRUE(std::holds_alternative<StopResponse>(answer->payload));
  EXPECT_LT(elapsed, std::chrono::milliseconds(150));
  pending.join();
  (*server)->Stop();
  std::filesystem::remove(directory);
}

}  // namespace
}  // namespace hbot
