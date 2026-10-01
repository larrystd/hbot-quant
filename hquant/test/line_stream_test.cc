#include "base/line_stream.h"

#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "base/error.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "gtest/gtest.h"

namespace hquant {
namespace {
namespace asio = boost::asio;
using Clock = std::chrono::steady_clock;

asio::awaitable<std::string> Echo(std::string line) {
  co_return line;
}

asio::awaitable<std::string> Slow(std::string line) {
  asio::steady_timer timer(co_await asio::this_coro::executor);
  timer.expires_after(std::chrono::milliseconds(200));
  co_await timer.async_wait(asio::use_awaitable);
  co_return line;
}

struct ClientResult {
  absl::Status connect;
  absl::Status send;
  std::optional<absl::StatusOr<std::string>> receive;
};

asio::awaitable<void> Request(LineClient& client, std::string line,
                               ClientResult& result,
                               std::chrono::milliseconds timeout =
                                   std::chrono::seconds(2)) {
  result.connect = co_await client.Connect(Clock::now() + timeout);
  if (!result.connect.ok()) co_return;
  result.send = co_await client.Send(std::move(line), Clock::now() + timeout);
  if (!result.send.ok()) co_return;
  result.receive = co_await client.Receive(Clock::now() + timeout);
}

asio::awaitable<void> IdleReceive(
    LineClient& client, absl::Status& connected,
    std::optional<absl::StatusOr<std::string>>& reply) {
  connected = co_await client.Connect(Clock::now() + std::chrono::seconds(1));
  if (connected.ok())
    reply = co_await client.Receive(Clock::now() + std::chrono::seconds(1));
}

asio::awaitable<void> ExpiredWrite(LineClient& client,
                                    absl::Status& result) {
  auto connected = co_await client.Connect(Clock::now() +
                                           std::chrono::seconds(1));
  if (!connected.ok()) {
    result = connected;
    co_return;
  }
  result = co_await client.Send("late", Clock::now() -
                                           std::chrono::milliseconds(1));
}

class RunningServer {
 public:
  RunningServer(LineEndpoint endpoint, LineServer::Handler handler,
                LineServer::Options options = {}) {
    auto opened = LineServer::Start(io, std::move(endpoint), std::move(handler),
                                    std::move(options));
    if (!opened.ok()) throw std::runtime_error(std::string(opened.status().message()));
    server = std::move(*opened);
    thread = std::thread([this] { io.run(); });
  }
  ~RunningServer() {
    server->Stop();
    thread.join();
  }
  uint16_t port() const { return server->bound_port(); }

 private:
  asio::io_context io;
  std::unique_ptr<LineServer> server;
  std::thread thread;
};

std::string TemporaryPath() {
  std::string directory = "/tmp/hqline_XXXXXX";
  if (!mkdtemp(directory.data())) throw std::runtime_error("mkdtemp failed");
  return directory + "/control.sock";
}

TEST(LineStreamTest, UnixAndTcpRoundTrip) {
  const std::string path = TemporaryPath();
  {
    RunningServer server(LineEndpoint::Unix(path), Echo);
    asio::io_context io;
    LineClient client(io, LineEndpoint::Unix(path));
    ClientResult result;
    asio::co_spawn(io, Request(client, "hello", result), asio::detached);
    io.run();
    ASSERT_TRUE(result.connect.ok()) << result.connect;
    ASSERT_TRUE(result.send.ok()) << result.send;
    ASSERT_TRUE(result.receive && result.receive->ok());
    EXPECT_EQ(**result.receive, "hello");
  }
  EXPECT_FALSE(std::filesystem::exists(path));
  std::filesystem::remove(std::filesystem::path(path).parent_path());
  {
    RunningServer server(LineEndpoint::Tcp("127.0.0.1", 0), Echo);
    asio::io_context io;
    LineClient client(io, LineEndpoint::Tcp("127.0.0.1", server.port()));
    ClientResult result;
    asio::co_spawn(io, Request(client, "tcp", result), asio::detached);
    io.run();
    ASSERT_TRUE(result.receive && result.receive->ok());
    EXPECT_EQ(**result.receive, "tcp");
  }
}

TEST(LineStreamTest, FrameLimitAndReadTimeout) {
  const std::string path = TemporaryPath();
  LineServer::Options options;
  options.max_frame = 8;
  options.invalid_reply = "INVALID";
  options.read_timeout = std::chrono::milliseconds(50);
  {
    RunningServer server(LineEndpoint::Unix(path), Echo, options);
    asio::io_context io;
    LineClient client(io, LineEndpoint::Unix(path));
    ClientResult result;
    asio::co_spawn(io, Request(client, "123456789", result), asio::detached);
    io.run();
    ASSERT_TRUE(result.receive && result.receive->ok());
    EXPECT_EQ(**result.receive, "INVALID");

    asio::io_context idle_io;
    LineClient idle(idle_io, LineEndpoint::Unix(path));
    absl::Status connected;
    std::optional<absl::StatusOr<std::string>> reply;
    asio::co_spawn(idle_io, IdleReceive(idle, connected, reply), asio::detached);
    idle_io.run();
    EXPECT_TRUE(connected.ok());
    ASSERT_TRUE(reply);
    EXPECT_EQ(CodeOf(reply->status()), ErrorCode::kNetNotConnected);
  }
  std::filesystem::remove(std::filesystem::path(path).parent_path());
}

TEST(LineStreamTest, ClientDeadlineAndPeerDisconnect) {
  RunningServer server(LineEndpoint::Tcp("127.0.0.1", 0), Slow);
  asio::io_context io;
  LineClient client(io, LineEndpoint::Tcp("127.0.0.1", server.port()));
  ClientResult result;
  asio::co_spawn(io, Request(client, "slow", result,
                              std::chrono::milliseconds(50)), asio::detached);
  io.run();
  ASSERT_TRUE(result.receive);
  EXPECT_EQ(CodeOf(result.receive->status()), ErrorCode::kNetTimeout);

  asio::io_context write_io;
  LineClient writer(write_io,
                    LineEndpoint::Tcp("127.0.0.1", server.port()));
  absl::Status write_status;
  asio::co_spawn(write_io, ExpiredWrite(writer, write_status), asio::detached);
  write_io.run();
  EXPECT_EQ(CodeOf(write_status), ErrorCode::kNetTimeout);

  asio::io_context dropped_io;
  asio::ip::tcp::socket dropped(dropped_io);
  dropped.connect({asio::ip::make_address("127.0.0.1"), server.port()});
  dropped.close();
}

TEST(LineStreamTest, ConcurrentLimitRepliesBusy) {
  LineServer::Options options;
  options.max_sessions = 1;
  options.busy_reply = "BUSY";
  RunningServer server(LineEndpoint::Tcp("127.0.0.1", 0), Slow, options);
  asio::io_context io;
  LineClient first(io, LineEndpoint::Tcp("127.0.0.1", server.port()));
  LineClient second(io, LineEndpoint::Tcp("127.0.0.1", server.port()));
  ClientResult first_result, second_result;
  asio::co_spawn(io, Request(first, "first", first_result), asio::detached);
  asio::steady_timer delay(io);
  delay.expires_after(std::chrono::milliseconds(30));
  delay.async_wait([&](const boost::system::error_code&) {
    asio::co_spawn(io, Request(second, "second", second_result),
                   asio::detached);
  });
  io.run();
  ASSERT_TRUE(first_result.receive && first_result.receive->ok());
  ASSERT_TRUE(second_result.receive && second_result.receive->ok());
  EXPECT_EQ(**first_result.receive, "first");
  EXPECT_EQ(**second_result.receive, "BUSY");
}

}  // namespace
}  // namespace hquant
