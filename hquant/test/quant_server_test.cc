#include "application/quant_server.h"

#include <string>
#include <variant>

int ProtocolContract() {
  hquant::ServerRequest request;
  request.request_id = 42;
  request.payload = hquant::HistoryRequest{20, "abc"};
  auto encoded = hquant::EncodeServerRequest(request);
  if (!encoded.ok()) return 1;
  auto decoded = hquant::DecodeServerRequest(*encoded);
  if (!decoded.ok() || decoded->request_id != 42 ||
      !std::holds_alternative<hquant::HistoryRequest>(decoded->payload) ||
      std::get<hquant::HistoryRequest>(decoded->payload).cursor != "abc")
    return 2;
  hquant::ServerResponse response;
  response.request_id = 42;
  response.payload = hquant::ServerError{hquant::ErrorCode::kServerBusy,
                                         "history \"timeout\"\n"};
  auto reply = hquant::EncodeServerResponse(response);
  if (!reply.ok() || reply->find("\"code\":-19008,\"name\":\"SERVER_BUSY\"") ==
                         std::string::npos)
    return 3;
  auto parsed = hquant::DecodeServerResponse(*reply);
  if (!parsed.ok() || parsed->request_id != 42 ||
      std::get<hquant::ServerError>(parsed->payload).code !=
          hquant::ErrorCode::kServerBusy ||
      std::get<hquant::ServerError>(parsed->payload).message !=
          "history \"timeout\"\n")
    return 4;
  auto old_request = hquant::DecodeServerRequest(
      "{\"schema_version\":1,\"request_id\":7,\"kind\":\"stop\"}");
  if (!old_request.ok() || old_request->schema_version != 1) return 5;
  auto old_response = hquant::DecodeServerResponse(
      "{\"schema_version\":1,\"request_id\":7,\"kind\":\"error\","
      "\"code\":\"busy\",\"message\":\"limit\"}");
  if (!old_response.ok() || old_response->schema_version != 1 ||
      std::get<hquant::ServerError>(old_response->payload).code !=
          hquant::ErrorCode::kServerBusy)
    return 6;
  auto new_response = hquant::EncodeServerResponse(hquant::ServerResponse{
      1, 7, hquant::ServerError{hquant::ErrorCode::kServerBusy, "limit"}});
  if (!new_response.ok() ||
      new_response->find("\"code\":\"busy\"") == std::string::npos)
    return 7;
  if (hquant::DecodeServerResponse(
          "{\"schema_version\":2,\"request_id\":7,\"kind\":\"error\","
          "\"code\":19008,\"name\":\"SERVER_TIMEOUT\","
          "\"message\":\"limit\"}")
          .ok())
    return 8;
  if (hquant::DecodeServerResponse(
          "{\"schema_version\":2,\"request_id\":7,\"kind\":\"error\","
          "\"code\":19999,\"name\":\"UNKNOWN\","
          "\"message\":\"limit\"}")
          .ok())
    return 9;
  if (hquant::DecodeServerRequest(
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

#include "cli/cli.h"
#include "gtest/gtest.h"
#include "storage/storage.h"

namespace hquant {
namespace {

TEST(QuantServerHistoryTest, DisplaysNegativeReasonNumbers) {
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

TEST(QuantServerTest, StopCanPassAnInFlightHistoryRequest) {
  EXPECT_EQ(::ProtocolContract(), 0);
  std::string directory = (std::filesystem::temp_directory_path() /
                           "hquant_quant_server_test_XXXXXX")
                              .string();
  ASSERT_NE(mkdtemp(directory.data()), nullptr);
  std::atomic<bool> history_entered{false};
  auto server = QuantServer::Start(
      ServerSocketPath(directory), [&](const ServerRequest& request) {
        ServerResponse response;
        if (std::holds_alternative<HistoryRequest>(request.payload)) {
          history_entered = true;
          std::this_thread::sleep_for(std::chrono::milliseconds(250));
          response.payload = HistoryResponse{"{\"rows\":[]}"};
        } else
          response.payload = StopResponse{true};
        return response;
      });
  ASSERT_TRUE(server.ok()) << server.status();
  ServerRequest legacy;
  legacy.schema_version = 1;
  legacy.request_id = 99;
  legacy.payload = StatusRequest{};
  auto legacy_reply = SendServerRequest(directory, legacy);
  ASSERT_TRUE(legacy_reply.ok()) << legacy_reply.status();
  EXPECT_EQ(legacy_reply->schema_version, 1u);
  ServerRequest history;
  history.request_id = 1;
  history.payload = HistoryRequest{};
  std::thread pending([&] {
    auto answer = SendServerRequest(directory, history);
    EXPECT_TRUE(answer.ok()) << answer.status();
  });
  for (int attempt = 0; attempt < 100 && !history_entered; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ASSERT_TRUE(history_entered);
  ServerRequest stop;
  stop.request_id = 2;
  stop.payload = StopRequest{};
  const auto started = std::chrono::steady_clock::now();
  auto answer = SendServerRequest(directory, stop);
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
