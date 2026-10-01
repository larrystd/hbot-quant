#include "application/control_server.h"

#include <string>
#include <variant>

int ProtocolContract() {
  hquant::ControlRequest request;
  request.request_id = 42;
  request.payload = hquant::HistoryRequest{20, "abc"};
  auto encoded = hquant::EncodeControlRequest(request);
  if (!encoded.ok()) return 1;
  auto decoded = hquant::DecodeControlRequest(*encoded);
  if (!decoded.ok() || decoded->request_id != 42 ||
      !std::holds_alternative<hquant::HistoryRequest>(decoded->payload) ||
      std::get<hquant::HistoryRequest>(decoded->payload).cursor != "abc")
    return 2;
  hquant::ControlResponse response;
  response.request_id = 42;
  response.payload = hquant::ControlError{hquant::ErrorCode::kControlBusy,
                                          "history \"timeout\"\n"};
  auto reply = hquant::EncodeControlResponse(response);
  if (!reply.ok() || reply->find("\"code\":-19008,\"name\":\"CONTROL_BUSY\"") ==
                         std::string::npos)
    return 3;
  auto parsed = hquant::DecodeControlResponse(*reply);
  if (!parsed.ok() || parsed->request_id != 42 ||
      std::get<hquant::ControlError>(parsed->payload).code !=
          hquant::ErrorCode::kControlBusy ||
      std::get<hquant::ControlError>(parsed->payload).message !=
          "history \"timeout\"\n")
    return 4;
  auto old_request = hquant::DecodeControlRequest(
      "{\"schema_version\":1,\"request_id\":7,\"kind\":\"stop\"}");
  if (!old_request.ok() || old_request->schema_version != 1) return 5;
  auto old_response = hquant::DecodeControlResponse(
      "{\"schema_version\":1,\"request_id\":7,\"kind\":\"error\","
      "\"code\":\"busy\",\"message\":\"limit\"}");
  if (!old_response.ok() || old_response->schema_version != 1 ||
      std::get<hquant::ControlError>(old_response->payload).code !=
          hquant::ErrorCode::kControlBusy)
    return 6;
  auto new_response = hquant::EncodeControlResponse(hquant::ControlResponse{
      1, 7, hquant::ControlError{hquant::ErrorCode::kControlBusy, "limit"}});
  if (!new_response.ok() ||
      new_response->find("\"code\":\"busy\"") == std::string::npos)
    return 7;
  if (hquant::DecodeControlResponse(
          "{\"schema_version\":2,\"request_id\":7,\"kind\":\"error\","
          "\"code\":19008,\"name\":\"CONTROL_TIMEOUT\","
          "\"message\":\"limit\"}")
          .ok())
    return 8;
  if (hquant::DecodeControlResponse(
          "{\"schema_version\":2,\"request_id\":7,\"kind\":\"error\","
          "\"code\":19999,\"name\":\"UNKNOWN\","
          "\"message\":\"limit\"}")
          .ok())
    return 9;
  if (hquant::DecodeControlRequest(
          "{\"schema_version\":3,\"request_id\":1,\"kind\":\"stop\"}")
          .ok())
    return 10;
  return 0;
}

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

#include "apps/bench_cli.h"
#include "boost/asio/steady_timer.hpp"
#include "boost/asio/this_coro.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "gtest/gtest.h"
#include "storage/storage.h"

namespace hquant {
namespace {

TEST(ControlServerHistoryTest, DisplaysNegativeReasonNumbers) {
  constexpr auto reason = ErrorCode::kStorageQueueFull;
  HistoryPage page;
  HistoryRecord prepared_record;
  PreparedOrder prepared;
  prepared.client_id = ClientOrderId{"P1"};
  prepared.request.base_amount = *Decimal::Parse("0.01");
  prepared_record.payload = prepared;
  page.rows.push_back(prepared_record);
  HistoryRecord decision;
  ActionRecord action;
  action.reason = reason;
  decision.payload = action;
  page.rows.push_back(decision);
  HistoryRecord gap;
  gap.payload = HistoryGap{RunId{1}, ShardId{0}, 1, 2, reason};
  page.rows.push_back(gap);
  page.incomplete_ranges.push_back(
      HistoryGap{RunId{1}, ShardId{0}, 1, 2, reason});
  const std::string json = HistoryJson(page);
  EXPECT_NE(json.find("\"kind\":\"prepared_order\""), std::string::npos);
  const std::string needle = "\"reason\":-17004";
  const auto first = json.find(needle);
  ASSERT_NE(first, std::string::npos);
  const auto second = json.find(needle, first + needle.size());
  ASSERT_NE(second, std::string::npos);
  EXPECT_NE(json.find(needle, second + needle.size()), std::string::npos);
}

TEST(ControlServerTest, StopCanPassAnInFlightHistoryRequest) {
  EXPECT_EQ(::ProtocolContract(), 0);
  std::string directory = (std::filesystem::temp_directory_path() /
                           "hquant_control_server_test_XXXXXX")
                              .string();
  ASSERT_NE(mkdtemp(directory.data()), nullptr);
  std::atomic<bool> history_entered{false};
  auto server = ControlServer::Start(
      ControlSocketPath(directory),
      [&](ControlRequest request) -> boost::asio::awaitable<ControlResponse> {
        ControlResponse response;
        if (std::holds_alternative<HistoryRequest>(request.payload)) {
          history_entered = true;
          boost::asio::steady_timer timer(
              co_await boost::asio::this_coro::executor);
          timer.expires_after(std::chrono::milliseconds(250));
          co_await timer.async_wait(boost::asio::use_awaitable);
          response.payload = HistoryResponse{"{\"rows\":[]}"};
        } else
          response.payload = StopResponse{true};
        co_return response;
      });
  ASSERT_TRUE(server.ok()) << server.status();
  ControlRequest legacy;
  legacy.schema_version = 1;
  legacy.request_id = 99;
  legacy.payload = StatusRequest{};
  auto legacy_reply = SendControlRequest(directory, legacy);
  ASSERT_TRUE(legacy_reply.ok()) << legacy_reply.status();
  EXPECT_EQ(legacy_reply->schema_version, 1u);
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
}  // namespace hquant
