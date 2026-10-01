#include "base/net_server.h"

#include <chrono>
#include <optional>
#include <string>

#include "base/net.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "gtest/gtest.h"

namespace {

boost::asio::awaitable<void> Exercise(boost::asio::io_context& io,
                                      hquant::HttpServer& server, uint16_t port,
                                      bool& valid) {
  hquant::HttpClient http(io, "127.0.0.1", std::to_string(port));
  hquant::HttpRequest request;
  request.method = "GET";
  request.target = "/api/v3/depth";
  request.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  auto response = co_await http.Send(std::move(request));
  hquant::WebSocketClient ws(io, "127.0.0.1", std::to_string(port),
                             "/stream?streams=btcusdt@depth");
  auto connected = co_await ws.Connect(std::chrono::steady_clock::now() +
                                       std::chrono::seconds(2));
  if (connected.ok()) {
    auto message = co_await ws.Read(std::chrono::steady_clock::now() +
                                    std::chrono::seconds(2));
    valid = response.ok() && response->status == 200 &&
            response->body == "snapshot" && message.ok() && *message == "depth";
  }
  server.Stop();
  io.stop();
}

TEST(NetServerTest, HttpAndWebSocketShareOnePort) {
  boost::asio::io_context io;
  auto server = hquant::HttpServer::Start(
      io, "127.0.0.1", 0,
      [](std::string method, std::string target) {
        EXPECT_EQ(method, "GET");
        EXPECT_EQ(target, "/api/v3/depth");
        return hquant::HttpResponse{200, "snapshot"};
      },
      [](uint64_t, uint64_t ordinal, std::string target)
          -> boost::asio::awaitable<std::optional<std::string>> {
        EXPECT_EQ(target, "/stream?streams=btcusdt@depth");
        if (ordinal == 0) co_return std::string("depth");
        co_return std::nullopt;
      });
  ASSERT_TRUE(server.ok()) << server.status();
  bool valid = false;
  boost::asio::co_spawn(io,
                        Exercise(io, **server, (*server)->bound_port(), valid),
                        boost::asio::detached);
  io.run();
  EXPECT_TRUE(valid);
}

}  // namespace
