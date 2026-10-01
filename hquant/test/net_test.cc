#include "base/net.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "base/error.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/beast/http.hpp"
#include "boost/beast/websocket.hpp"
#include "gtest/gtest.h"

namespace hquant {
namespace {

namespace asio = boost::asio;
namespace http = boost::beast::http;
using Tcp = asio::ip::tcp;
using NetClock = std::chrono::steady_clock;

HttpRequest Request(std::chrono::milliseconds timeout) {
  HttpRequest request;
  request.method = "GET";
  request.target = "/test";
  request.deadline = NetClock::now() + timeout;
  return request;
}

TEST(HttpClientTest, ReusesKeepAliveConnection) {
  asio::io_context server_io;
  Tcp::acceptor acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const std::string port = std::to_string(acceptor.local_endpoint().port());
  std::thread server([&] {
    Tcp::socket socket(server_io);
    acceptor.accept(socket);
    for (int i = 0; i < 2; ++i) {
      boost::beast::flat_buffer buffer;
      http::request<http::string_body> request;
      http::read(socket, buffer, request);
      http::response<http::string_body> response{http::status::ok, 11};
      response.keep_alive(true);
      response.body() = std::to_string(i + 1);
      if (i == 0) response.set(http::field::retry_after, "7");
      response.prepare_payload();
      http::write(socket, response);
    }
  });

  asio::io_context io;
  HttpClient client(io, "127.0.0.1", port);
  std::vector<absl::StatusOr<HttpResponse>> results;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        results.push_back(
            co_await client.Send(Request(std::chrono::seconds(2))));
        results.push_back(
            co_await client.Send(Request(std::chrono::seconds(2))));
      },
      asio::detached);
  io.run();
  server.join();

  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(results[0].ok()) << results[0].status();
  ASSERT_TRUE(results[1].ok()) << results[1].status();
  EXPECT_EQ(results[0]->status, 200);
  EXPECT_EQ(results[0]->body, "1");
  EXPECT_EQ(results[0]->retry_after, std::chrono::seconds(7));
  EXPECT_TRUE(std::any_of(results[0]->headers.begin(), results[0]->headers.end(),
                          [](const auto& header) {
                            return header.first == "Retry-After" &&
                                   header.second == "7";
                          }));
  EXPECT_EQ(results[1]->body, "2");
  EXPECT_EQ(results[1]->retry_after,
            std::chrono::steady_clock::duration::zero());
}

TEST(HttpClientTest, ReconnectsOnceForGetAfterStaleKeepAlive) {
  asio::io_context server_io;
  Tcp::acceptor acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const std::string port = std::to_string(acceptor.local_endpoint().port());
  unsigned accepted = 0;
  std::thread server([&] {
    for (int i = 0; i < 2; ++i) {
      Tcp::socket socket(server_io);
      acceptor.accept(socket);
      ++accepted;
      boost::beast::flat_buffer buffer;
      http::request<http::string_body> request;
      http::read(socket, buffer, request);
      EXPECT_EQ(request.method(), http::verb::get);
      http::response<http::string_body> response{http::status::ok, 11};
      response.keep_alive(true);
      response.body() = std::to_string(i + 1);
      response.prepare_payload();
      http::write(socket, response);
      if (i == 0) {
        boost::system::error_code ignored;
        socket.shutdown(Tcp::socket::shutdown_both, ignored);
        socket.close(ignored);
      }
    }
  });

  asio::io_context io;
  HttpClient client(io, "127.0.0.1", port);
  std::vector<absl::StatusOr<HttpResponse>> results;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        results.push_back(
            co_await client.Send(Request(std::chrono::seconds(2))));
        results.push_back(
            co_await client.Send(Request(std::chrono::seconds(2))));
      },
      asio::detached);
  io.run();
  server.join();

  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(results[0].ok()) << results[0].status();
  ASSERT_TRUE(results[1].ok()) << results[1].status();
  EXPECT_EQ(results[0]->body, "1");
  EXPECT_EQ(results[1]->body, "2");
  EXPECT_EQ(accepted, 2);
}

TEST(HttpClientTest, DoesNotRetryPostAfterStaleKeepAlive) {
  asio::io_context server_io;
  Tcp::acceptor acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const std::string port = std::to_string(acceptor.local_endpoint().port());
  std::thread server([&] {
    Tcp::socket socket(server_io);
    acceptor.accept(socket);
    boost::beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    http::response<http::string_body> response{http::status::ok, 11};
    response.keep_alive(true);
    response.body() = "ready";
    response.prepare_payload();
    http::write(socket, response);
    boost::system::error_code ignored;
    socket.shutdown(Tcp::socket::shutdown_both, ignored);
    socket.close(ignored);
  });

  asio::io_context io;
  HttpClient client(io, "127.0.0.1", port);
  std::vector<absl::StatusOr<HttpResponse>> results;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        results.push_back(
            co_await client.Send(Request(std::chrono::seconds(2))));
        auto post = Request(std::chrono::seconds(2));
        post.method = "POST";
        post.body = "order";
        results.push_back(co_await client.Send(std::move(post)));
      },
      asio::detached);
  io.run();
  server.join();

  ASSERT_EQ(results.size(), 2);
  ASSERT_TRUE(results[0].ok()) << results[0].status();
  EXPECT_EQ(results[1].status().code(), absl::StatusCode::kUnavailable);
}

TEST(HttpClientTest, ResponseDeadlineClosesConnection) {
  asio::io_context server_io;
  Tcp::acceptor acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const std::string port = std::to_string(acceptor.local_endpoint().port());
  std::thread server([&] {
    Tcp::socket socket(server_io);
    acceptor.accept(socket);
    boost::beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  });

  asio::io_context io;
  HttpClient client(io, "127.0.0.1", port);
  absl::Status status;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        auto result =
            co_await client.Send(Request(std::chrono::milliseconds(30)));
        status = result.ok() ? absl::OkStatus() : result.status();
      },
      asio::detached);
  io.run();
  server.join();
  EXPECT_EQ(status.code(), absl::StatusCode::kDeadlineExceeded);
  EXPECT_FALSE(client.Connected());
}

TEST(HttpClientTest, CancelInterruptsRead) {
  asio::io_context server_io;
  Tcp::acceptor acceptor(server_io, Tcp::endpoint(Tcp::v4(), 0));
  const std::string port = std::to_string(acceptor.local_endpoint().port());
  std::thread server([&] {
    Tcp::socket socket(server_io);
    acceptor.accept(socket);
    boost::beast::flat_buffer buffer;
    http::request<http::string_body> request;
    http::read(socket, buffer, request);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  });

  asio::io_context io;
  HttpClient client(io, "127.0.0.1", port);
  asio::steady_timer timer(io);
  timer.expires_after(std::chrono::milliseconds(30));
  timer.async_wait([&](const boost::system::error_code& ec) {
    if (!ec) client.Cancel();
  });
  absl::Status status;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        auto result = co_await client.Send(Request(std::chrono::seconds(2)));
        status = result.ok() ? absl::OkStatus() : result.status();
      },
      asio::detached);
  io.run();
  server.join();
  EXPECT_EQ(status.code(), absl::StatusCode::kCancelled);
}

TEST(HttpClientTest, RejectsInvalidCaFileBeforeConnecting) {
  asio::io_context io;
  HttpClient client(io, "127.0.0.1", "1",
                    TlsConfig{true, true, "/missing-ca-file.pem", "localhost"});
  absl::Status status;
  asio::co_spawn(
      io,
      [&]() -> asio::awaitable<void> {
        auto result = co_await client.Send(Request(std::chrono::seconds(1)));
        status = result.ok() ? absl::OkStatus() : result.status();
      },
      asio::detached);
  io.run();
  EXPECT_EQ(CodeOf(status), ErrorCode::kNetTlsConfigInvalid);
}

namespace beast = boost::beast;
namespace websocket = beast::websocket;

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
          const auto deadline = NetClock::now() + std::chrono::seconds(2);
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
            co_await client.Connect(NetClock::now() + std::chrono::seconds(2));
        if (!connect_status.ok()) co_return;
        auto result = co_await client.Read(NetClock::now() +
                                           std::chrono::milliseconds(30));
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
}  // namespace hquant
