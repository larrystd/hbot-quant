#include "apps/bench_control.h"

#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <variant>

#include "application/control_server.h"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/this_coro.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "gtest/gtest.h"

namespace {

TEST(BenchControlTest, RejectsInvalidModeMixAndRate) {
  using hquant::ParseControlBenchArguments;
  const std::array<std::string_view, 4> valid{"--state-dir", "/tmp/test",
                                              "--mix", "status=0,history=100"};
  auto parsed = ParseControlBenchArguments(valid);
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  EXPECT_EQ(parsed->status_percent, 0);
  const std::array<std::string_view, 6> bad_mix{
      "--state-dir",          "/tmp/test",  "--mix",
      "status=30,history=30", "--duration", "1"};
  EXPECT_FALSE(ParseControlBenchArguments(bad_mix).ok());
  const std::array<std::string_view, 4> no_open_rate{"--state-dir", "/tmp/test",
                                                     "--mode", "open"};
  EXPECT_FALSE(ParseControlBenchArguments(no_open_rate).ok());
  const std::array<std::string_view, 8> open{
      "--state-dir", "/tmp/test", "--mode",        "open",
      "--rate",      "100",       "--connections", "2"};
  EXPECT_TRUE(ParseControlBenchArguments(open).ok());
}

TEST(BenchControlTest, ClassifiesBusyResponses) {
  std::string directory = "/tmp/hquant_bench_control_XXXXXX";
  ASSERT_NE(mkdtemp(directory.data()), nullptr);
  const auto path = std::filesystem::path(directory) / "control.sock";
  auto server = hquant::ControlServer::Start(
      path.string(),
      [](hquant::ControlRequest request)
          -> boost::asio::awaitable<hquant::ControlResponse> {
        boost::asio::steady_timer timer(
            co_await boost::asio::this_coro::executor);
        timer.expires_after(std::chrono::milliseconds(200));
        co_await timer.async_wait(boost::asio::use_awaitable);
        hquant::ControlResponse reply;
        reply.request_id = request.request_id;
        reply.payload = hquant::StatusResponse{"{}"};
        co_return reply;
      });
  ASSERT_TRUE(server.ok()) << server.status();
  hquant::ControlBenchOptions options;
  options.state_dir = directory;
  options.connections = 100;
  options.threads = 4;
  options.duration_seconds = 1;
  options.status_percent = 100;
  options.json = true;
  auto report = hquant::RunControlBench(options);
  ASSERT_TRUE(report.ok()) << report.status();
  EXPECT_NE(report->find("\"CONTROL_BUSY\":"), std::string::npos) << *report;
  (*server)->Stop();
  std::filesystem::remove_all(directory);
}

}  // namespace
