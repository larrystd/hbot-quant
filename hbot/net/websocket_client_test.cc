#include "hbot/net/websocket_client.h"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/beast/websocket.hpp"
#include "gtest/gtest.h"

namespace hbot {
namespace {
namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using Tcp = asio::ip::tcp;
using Clock = std::chrono::steady_clock;

TEST(WebSocketClientTest, EchoAndReconnectToSecondSession) {
  asio::io_context server_io;
  Tcp::acceptor acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const std::string port = std::to_string(acceptor.local_endpoint().port());
  std::thread server([&] {
    for (int i = 0; i < 2; ++i) {
      Tcp::socket socket(server_io);
      acceptor.accept(socket);
      websocket::stream<Tcp::socket> ws(std::move(socket));
      ws.accept();
      beast::flat_buffer buffer;
      ws.read(buffer);
      ws.text(true);
      ws.write(buffer.data());
      ws.close(websocket::close_code::normal);
    }
  });

  asio::io_context io;
  WebSocketClient client(io, "127.0.0.1", port, "/stream");
  std::vector<absl::Status> statuses;
  std::vector<absl::StatusOr<std::string>> replies;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        for (int i = 0; i < 2; ++i) {
          const auto deadline = Clock::now() + std::chrono::seconds(2);
          statuses.push_back(i == 0 ? co_await client.Connect(deadline)
                                    : co_await client.Reconnect(deadline));
          if (!statuses.back().ok()) co_return;
          statuses.push_back(
              co_await client.WriteText("ping" + std::to_string(i), deadline));
          if (!statuses.back().ok()) co_return;
          replies.push_back(co_await client.Read(deadline));
          if (!replies.back().ok()) co_return;
          statuses.push_back(co_await client.Close(deadline));
        }
      },
      asio::detached);
  io.run();
  server.join();

  ASSERT_EQ(statuses.size(), 6);
  for (const auto& status : statuses) EXPECT_TRUE(status.ok()) << status;
  ASSERT_EQ(replies.size(), 2);
  ASSERT_TRUE(replies[0].ok()) << replies[0].status();
  ASSERT_TRUE(replies[1].ok()) << replies[1].status();
  EXPECT_EQ(*replies[0], "ping0");
  EXPECT_EQ(*replies[1], "ping1");
  EXPECT_FALSE(client.Connected());
}

TEST(WebSocketClientTest, ReadDeadlineClosesSession) {
  asio::io_context server_io;
  Tcp::acceptor acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const std::string port = std::to_string(acceptor.local_endpoint().port());
  std::thread server([&] {
    Tcp::socket socket(server_io);
    acceptor.accept(socket);
    websocket::stream<Tcp::socket> ws(std::move(socket));
    ws.accept();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  });

  asio::io_context io;
  WebSocketClient client(io, "127.0.0.1", port, "/stream");
  absl::Status connect_status, read_status;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        connect_status =
            co_await client.Connect(Clock::now() + std::chrono::seconds(2));
        if (!connect_status.ok()) co_return;
        auto result =
            co_await client.Read(Clock::now() + std::chrono::milliseconds(30));
        read_status = result.ok() ? absl::OkStatus() : result.status();
      },
      asio::detached);
  io.run();
  server.join();

  EXPECT_TRUE(connect_status.ok()) << connect_status;
  EXPECT_EQ(read_status.code(), absl::StatusCode::kDeadlineExceeded);
  EXPECT_FALSE(client.Connected());
}

}  // namespace
}  // namespace hbot
