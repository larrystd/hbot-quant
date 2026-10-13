#include "hquant/venue/binance/trade_client.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "boost/asio/co_spawn.hpp"
#include "boost/asio/detached.hpp"
#include "boost/asio/dispatch.hpp"
#include "boost/asio/post.hpp"
#include "boost/asio/redirect_error.hpp"
#include "boost/asio/use_awaitable.hpp"
#include "hquant/base/fixed.h"
#include "hquant/venue/binance/auth.h"
#include "hquant/venue/binance/rules.h"
#include "simdjson.h"

namespace hquant::v1 {
namespace {

template <typename Callback>
Callback OnAccount(boost::asio::io_context& io, Callback callback) {
  return [&io, callback = std::move(callback)](auto value) mutable {
    boost::asio::post(
        io, [callback = std::move(callback), value = std::move(value)]() mutable {
          callback(std::move(value));
        });
  };
}

absl::StatusOr<simdjson::dom::element> Parse(simdjson::dom::parser& parser,
                                             std::string_view json) {
  simdjson::dom::element root;
  if (parser.parse(json).get(root)) {
    return absl::DataLossError("invalid Binance REST JSON");
  }
  return root;
}

absl::StatusOr<std::string_view> Text(simdjson::dom::element root,
                                      const char* field) {
  std::string_view value;
  if (root[field].get(value)) {
    return absl::DataLossError(std::string("missing Binance field ") + field);
  }
  return value;
}

absl::StatusOr<uint64_t> Number(simdjson::dom::element root,
                                const char* field) {
  uint64_t value = 0;
  if (root[field].get(value)) {
    return absl::DataLossError(std::string("missing Binance field ") + field);
  }
  return value;
}

absl::StatusOr<uint64_t> Fixed(simdjson::dom::element root, const char* field) {
  auto text = Text(root, field);
  if (!text.ok()) return text.status();
  return ParseFixed(*text);
}

std::string ErrorText(const HttpResponse& response) {
  simdjson::dom::parser parser;
  auto root = Parse(parser, response.body);
  if (root.ok()) {
    auto message = Text(*root, "msg");
    if (message.ok()) return std::string(*message);
  }
  return "Binance HTTP " + std::to_string(response.status);
}

bool ExecutionMayBeUnknown(const HttpResponse& response) {
  simdjson::dom::parser parser;
  auto root = Parse(parser, response.body);
  if (!root.ok()) return false;
  int64_t code = 0;
  if ((*root)["code"].get(code)) return false;
  return code == -1000 || code == -1001 || code == -1006 || code == -1007;
}

absl::StatusOr<OrderReport> ParseOrder(simdjson::dom::element root) {
  auto symbol = Text(root, "symbol");
  auto client = Text(root, "clientOrderId");
  auto exchange = Number(root, "orderId");
  auto side = Text(root, "side");
  auto price = Fixed(root, "price");
  auto quantity = Fixed(root, "origQty");
  auto executed = Fixed(root, "executedQty");
  auto status = Text(root, "status");
  if (!symbol.ok()) return symbol.status();
  if (!client.ok()) return client.status();
  if (!exchange.ok()) return exchange.status();
  if (!side.ok()) return side.status();
  if (!price.ok()) return price.status();
  if (!quantity.ok()) return quantity.status();
  if (!executed.ok()) return executed.status();
  if (!status.ok()) return status.status();
  if (symbol->empty() || client->empty() || *executed > *quantity ||
      (*side != "BUY" && *side != "SELL")) {
    return absl::DataLossError("invalid Binance order identity");
  }
  OrderStatus mapped;
  if (*status == "NEW")
    mapped = OrderStatus::Accepted;
  else if (*status == "PARTIALLY_FILLED")
    mapped = OrderStatus::PartiallyFilled;
  else if (*status == "FILLED")
    mapped = OrderStatus::Filled;
  else if (*status == "CANCELED" || *status == "EXPIRED")
    mapped = OrderStatus::Cancelled;
  else if (*status == "REJECTED")
    mapped = OrderStatus::Rejected;
  else
    return absl::DataLossError("unsupported Binance order status");
  Order order;
  order.market = MarketId{std::string(*symbol)};
  order.client_order_id = ClientOrderId{std::string(*client)};
  order.exchange_order_id = ExchangeOrderId{*exchange};
  order.side = *side == "BUY" ? Side::Buy : Side::Sell;
  order.price_nanos = *price;
  order.quantity_nanos = *quantity;
  order.remaining_nanos = *quantity - *executed;
  uint64_t update_time = 0;
  (void)root["updateTime"].get(update_time);
  return OrderReport{std::move(order), mapped, {}, update_time};
}

absl::StatusOr<OrderReport> ParseQueryOrder(std::string_view json,
                                            const QueryCommand& query) {
  simdjson::dom::parser parser;
  auto root = Parse(parser, json);
  if (!root.ok()) return root.status();
  auto report = ParseOrder(*root);
  if (!report.ok()) return report.status();
  if (report->order.market != query.market ||
      report->order.client_order_id != query.client_order_id ||
      (query.exchange_order_id.value != 0 &&
       report->order.exchange_order_id != query.exchange_order_id)) {
    return absl::DataLossError("order query identity mismatch");
  }
  return report;
}

absl::StatusOr<std::vector<OrderReport>> ParseOpenOrders(
    std::string_view json) {
  simdjson::dom::parser parser;
  auto root = Parse(parser, json);
  if (!root.ok()) return root.status();
  simdjson::dom::array rows;
  if (root->get(rows)) return absl::DataLossError("invalid open orders");
  std::vector<OrderReport> result;
  for (simdjson::dom::element row : rows) {
    auto report = ParseOrder(row);
    if (!report.ok()) return report.status();
    if (report->state != OrderStatus::Accepted &&
        report->state != OrderStatus::PartiallyFilled) {
      return absl::DataLossError("terminal order in open orders response");
    }
    result.push_back(std::move(*report));
  }
  return result;
}

absl::StatusOr<std::vector<BalanceUpdate>> ParseBalances(
    std::string_view json) {
  simdjson::dom::parser parser;
  auto root = Parse(parser, json);
  if (!root.ok()) return root.status();
  simdjson::dom::array rows;
  if ((*root)["balances"].get(rows)) {
    return absl::DataLossError("missing Binance balances");
  }
  std::vector<BalanceUpdate> result;
  for (simdjson::dom::element row : rows) {
    auto asset = Text(row, "asset");
    auto free = Fixed(row, "free");
    auto locked = Fixed(row, "locked");
    if (!asset.ok()) return asset.status();
    if (!free.ok()) return free.status();
    if (!locked.ok()) return locked.status();
    auto total = CheckedAdd(*free, *locked);
    if (!total.ok()) return total.status();
    result.push_back(
        BalanceUpdate{std::string(*asset), *total, *free, *locked});
  }
  return result;
}

absl::StatusOr<std::vector<Fill>> ParseTrades(std::string_view json,
                                              const QueryCommand& query) {
  if (query.exchange_order_id.value == 0) {
    return absl::InvalidArgumentError("exchange order id required for trades");
  }
  simdjson::dom::parser parser;
  auto root = Parse(parser, json);
  if (!root.ok()) return root.status();
  simdjson::dom::array rows;
  if (root->get(rows)) return absl::DataLossError("invalid Binance trades");
  std::vector<Fill> result;
  for (simdjson::dom::element row : rows) {
    auto symbol = Text(row, "symbol");
    auto order_id = Number(row, "orderId");
    auto trade_id = Number(row, "id");
    auto price = Fixed(row, "price");
    auto quantity = Fixed(row, "qty");
    auto gross = Fixed(row, "quoteQty");
    auto fee = Fixed(row, "commission");
    auto fee_asset = Text(row, "commissionAsset");
    auto transaction_ms = Number(row, "time");
    bool is_buyer = false;
    if (!symbol.ok()) return symbol.status();
    if (!order_id.ok()) return order_id.status();
    if (!trade_id.ok()) return trade_id.status();
    if (!price.ok()) return price.status();
    if (!quantity.ok()) return quantity.status();
    if (!gross.ok()) return gross.status();
    if (!fee.ok()) return fee.status();
    if (!fee_asset.ok()) return fee_asset.status();
    if (!transaction_ms.ok()) return transaction_ms.status();
    if (row["isBuyer"].get(is_buyer) || *symbol != query.market.symbol ||
        *order_id != query.exchange_order_id.value || *trade_id == 0) {
      return absl::DataLossError("trade query identity mismatch");
    }
    result.push_back(Fill{{},
                          is_buyer ? Side::Buy : Side::Sell,
                          *trade_id,
                          *price,
                          *quantity,
                          *gross,
                          *fee,
                          query.market,
                          query.client_order_id,
                          std::string(*fee_asset),
                          *transaction_ms});
  }
  return result;
}

}  // namespace

BinanceTradeClient::BinanceTradeClient(
    boost::asio::io_context& io, const AccountConfig& config,
    std::string api_key, std::string api_secret,
    BinanceUserStream::EventSink on_event,
    BinanceUserStream::HealthSink on_health, TradePerf* perf,
    size_t queue_capacity, std::shared_ptr<RequestScheduler> scheduler,
    std::string rule_symbol, boost::asio::io_context* shard_rest_io,
    bool start_private_stream)
    : io_(io),
      rest_io_(shard_rest_io ? *shard_rest_io : owned_rest_io_),
      rest_guard_(boost::asio::make_work_guard(rest_io_)),
      account_guard_(boost::asio::make_work_guard(io_)),
      private_guard_(boost::asio::make_work_guard(private_io_)),
      user_stream_(private_io_, config, api_key, api_secret, std::move(on_event),
                   std::move(on_health), perf),
      scheduler_(scheduler ? std::move(scheduler)
                           : std::make_shared<RequestScheduler>(
                                 config.request_weight_per_minute,
                                 config.order_placements_per_ten_seconds)),
      api_key_(std::move(api_key)),
      api_secret_(std::move(api_secret)),
      rule_symbol_(std::move(rule_symbol)),
      private_stream_enabled_(start_private_stream),
      queue_capacity_(queue_capacity),
      submit_queue_timeout_(config.mode == TradingMode::PressureSim
                                ? std::chrono::seconds(30)
                                : std::chrono::seconds(5)),
      perf_(perf) {
  rest_lanes_.reserve(config.rest_connections);
  for (size_t i = 0; i < config.rest_connections; ++i)
    rest_lanes_.push_back(std::make_unique<RestLane>(rest_io_, config));
  if (private_stream_enabled_)
    user_stream_.Start();
  else
    private_guard_.reset();
  try {
    if (!shard_rest_io)
      rest_worker_ = std::thread([this] { rest_io_.run(); });
    if (private_stream_enabled_)
      private_worker_ = std::thread([this] { private_io_.run(); });
  } catch (...) {
    rest_guard_.reset();
    private_guard_.reset();
    if (!shard_rest_io) rest_io_.stop();
    private_io_.stop();
    if (rest_worker_.joinable()) rest_worker_.join();
    if (private_worker_.joinable()) private_worker_.join();
    throw;
  }
}

BinanceTradeClient::~BinanceTradeClient() {
  if (!stopped_.load()) Stop();
  if (rest_worker_.joinable()) rest_worker_.join();
  if (private_worker_.joinable()) private_worker_.join();
}

bool BinanceTradeClient::Enqueue(Job&& job) {
  const size_t queued = accepted_queued_jobs_.load();
  if (stopped_.load() || queued >= queue_capacity_) {
    ++queue_rejections_;
    return false;
  }
  const size_t reserved =
      queue_capacity_ >= 16 ? std::max<size_t>(1, queue_capacity_ / 8) : 0;
  if (job.kind == Job::Kind::Submit &&
      queued >= queue_capacity_ - 2 * reserved) {
    ++queue_rejections_;
    return false;
  }
  if (job.kind == Job::Kind::Cancel &&
      queued >= queue_capacity_ - reserved) {
    ++queue_rejections_;
    return false;
  }
  job.enqueued = std::chrono::steady_clock::now();
  if (perf_) job.enqueued_ns = BenchClockNowNs();
  if (job.query_done)
    job.query_done = OnAccount(io_, std::move(job.query_done));
  if (job.trades_done)
    job.trades_done = OnAccount(io_, std::move(job.trades_done));
  if (job.balances_done)
    job.balances_done = OnAccount(io_, std::move(job.balances_done));
  if (job.open_orders_done)
    job.open_orders_done = OnAccount(io_, std::move(job.open_orders_done));
  if (job.rules_done)
    job.rules_done = OnAccount(io_, std::move(job.rules_done));
  const size_t accepted = accepted_queued_jobs_.fetch_add(1) + 1;
  size_t peak = queue_peak_.load();
  while (accepted > peak &&
         !queue_peak_.compare_exchange_weak(peak, accepted)) {}
  if (rest_io_.get_executor().running_in_this_thread()) {
    QueueOnRest(std::move(job));
  } else {
    boost::asio::dispatch(rest_io_, [this, job = std::move(job)]() mutable {
      QueueOnRest(std::move(job));
    });
  }
  return true;
}

void BinanceTradeClient::QueueOnRest(Job&& job) {
  if (perf_) job.posted_ns = BenchClockNowNs();
  job.queued_before_post = queued_jobs_;
  job.active_lanes_on_post = active_lanes_;
  queues_[Priority(job.kind)].push_back(std::move(job));
  ++queued_jobs_;
  for (const auto& lane : rest_lanes_)
    if (lane->waiting_for_schedule) lane->schedule_timer.cancel();
  StartDrains();
}

void BinanceTradeClient::StartDrains() {
  size_t remaining = queued_jobs_;
  for (const auto& lane : rest_lanes_) {
    if (remaining == 0) break;
    if (lane->draining) continue;
    lane->draining = true;
    ++active_lanes_;
    boost::asio::co_spawn(rest_io_, Drain(*lane), boost::asio::detached);
    --remaining;
  }
}

TradeQueueHealth BinanceTradeClient::QueueHealth() const {
  return TradeQueueHealth{accepted_queued_jobs_.load(), queue_peak_.load(),
                          queue_rejections_.load()};
}

RequestClass BinanceTradeClient::Classify(Job::Kind kind) {
  switch (kind) {
    case Job::Kind::Submit:
      return RequestClass::Submit;
    case Job::Kind::Cancel:
      return RequestClass::Cancel;
    case Job::Kind::Query:
      return RequestClass::OrderQuery;
    case Job::Kind::Trades:
      return RequestClass::TradeQuery;
    case Job::Kind::Balances:
      return RequestClass::Balances;
    case Job::Kind::OpenOrders:
      return RequestClass::OpenOrders;
    case Job::Kind::Rules:
      return RequestClass::Rules;
  }
  return RequestClass::Submit;
}

size_t BinanceTradeClient::Priority(Job::Kind kind) {
  switch (kind) {
    case Job::Kind::Cancel: return 0;
    case Job::Kind::Query: return 1;
    case Job::Kind::Trades: return 2;
    case Job::Kind::OpenOrders:
    case Job::Kind::Rules: return 3;
    case Job::Kind::Balances: return 4;
    case Job::Kind::Submit: return 5;
  }
  return 5;
}

std::deque<BinanceTradeClient::Job>& BinanceTradeClient::NextQueue() {
  for (auto& queue : queues_)
    if (!queue.empty()) return queue;
  std::terminate();
}

BinanceTradeClient::Job BinanceTradeClient::PopNextJob() {
  auto& queue = NextQueue();
  Job job = std::move(queue.front());
  queue.pop_front();
  --queued_jobs_;
  --accepted_queued_jobs_;
  return job;
}

bool BinanceTradeClient::Submit(SubmitCommand command, Completion done,
                                WriteStarted write_started) {
  Job job;
  job.kind = Job::Kind::Submit;
  job.submit = std::move(command);
  job.done = std::move(done);
  job.write_started = std::move(write_started);
  return Enqueue(std::move(job));
}

bool BinanceTradeClient::Cancel(CancelCommand command, Completion done,
                                WriteStarted write_started) {
  Job job;
  job.kind = Job::Kind::Cancel;
  job.cancel = std::move(command);
  job.done = std::move(done);
  job.write_started = std::move(write_started);
  return Enqueue(std::move(job));
}

bool BinanceTradeClient::Query(QueryCommand command, QueryCompletion done) {
  Job job;
  job.kind = Job::Kind::Query;
  job.query = std::move(command);
  job.query_done = std::move(done);
  return Enqueue(std::move(job));
}

bool BinanceTradeClient::QueryTrades(QueryCommand command,
                                     TradesCompletion done) {
  Job job;
  job.kind = Job::Kind::Trades;
  job.query = std::move(command);
  job.trades_done = std::move(done);
  return Enqueue(std::move(job));
}

bool BinanceTradeClient::QueryBalances(BalancesCompletion done) {
  Job job;
  job.kind = Job::Kind::Balances;
  job.balances_done = std::move(done);
  return Enqueue(std::move(job));
}

bool BinanceTradeClient::QueryOpenOrders(OpenOrdersCompletion done) {
  Job job;
  job.kind = Job::Kind::OpenOrders;
  job.open_orders_done = std::move(done);
  return Enqueue(std::move(job));
}

bool BinanceTradeClient::QueryRules(RulesCompletion done) {
  Job job;
  job.kind = Job::Kind::Rules;
  job.rules_done = std::move(done);
  return Enqueue(std::move(job));
}

HttpRequest BinanceTradeClient::MakeRequest(Job& job) const {
  HttpRequest request;
  if (job.kind == Job::Kind::Rules) {
    request.method = "GET";
    request.target = "/api/v3/exchangeInfo";
    if (!rule_symbol_.empty()) request.target += "?symbol=" + rule_symbol_;
    request.deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    return request;
  }
  std::string parameters;
  std::string path = "/api/v3/order";
  if (job.kind == Job::Kind::Submit) {
    request.method = "POST";
    parameters = "symbol=" + job.submit.market.symbol + "&side=" +
                 (job.submit.order.side == Side::Buy ? "BUY" : "SELL") +
                 "&type=LIMIT&timeInForce=GTC&quantity=" +
                 BinanceDecimal(job.submit.order.quantity_nanos) +
                 "&price=" + BinanceDecimal(job.submit.order.price_nanos) +
                 "&newClientOrderId=" + job.submit.client_order_id.value;
  } else if (job.kind == Job::Kind::Cancel) {
    request.method = "DELETE";
    parameters = "symbol=" + job.cancel.market.symbol +
                 "&origClientOrderId=" + job.cancel.client_order_id.value;
  } else if (job.kind == Job::Kind::Query) {
    request.method = "GET";
    parameters = "symbol=" + job.query.market.symbol +
                 "&origClientOrderId=" + job.query.client_order_id.value;
  } else if (job.kind == Job::Kind::Trades) {
    request.method = "GET";
    path = "/api/v3/myTrades";
    parameters = "symbol=" + job.query.market.symbol + "&orderId=" +
                 std::to_string(job.query.exchange_order_id.value) +
                 "&limit=1000";
    if (job.from_trade_id != 0) {
      parameters += "&fromId=" + std::to_string(job.from_trade_id);
    }
  } else if (job.kind == Job::Kind::Balances) {
    request.method = "GET";
    path = "/api/v3/account";
    parameters = "omitZeroBalances=false";
  } else {
    request.method = "GET";
    path = "/api/v3/openOrders";
  }
  if (!parameters.empty()) parameters += '&';
  parameters +=
      "recvWindow=5000&timestamp=" + std::to_string(BinanceTimestampMs());
  request.target = path + "?" + parameters + "&signature=" +
                   BinanceHmacSignature(api_secret_, parameters);
  request.headers.emplace_back("X-MBX-APIKEY", api_key_);
  request.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  if (job.kind == Job::Kind::Submit || job.kind == Job::Kind::Cancel) {
    request.on_write_start = [this, &job] {
      if (job.write_started) boost::asio::post(io_, job.write_started);
      if (perf_) job.write_start_ns = BenchClockNowNs();
    };
    if (perf_) request.write_trace = &job.write_trace;
  }
  request.on_write = [this, &job](auto begin, auto done) {
    job.write_begin = begin;
    job.write_done = done;
    if (perf_) job.write_done_ns = BenchClockNowNs();
  };
  return request;
}

boost::asio::awaitable<void> BinanceTradeClient::Drain(RestLane& lane) {
  while (queued_jobs_ != 0 && !stopped_.load()) {
    Job& selected = NextQueue().front();
    const RequestClass kind = Classify(selected.kind);
    const auto now = std::chrono::steady_clock::now();
    if (selected.kind == Job::Kind::Submit &&
        now - selected.enqueued >= submit_queue_timeout_) {
      Job expired = PopNextJob();
      CompleteTrade(expired,
                    TradeResponse{TradeResponse::Result::NotSent, {},
                                  "local submit queue expired before send"});
      continue;
    }
    const auto delay = scheduler_->Reserve(kind, now);
    if (delay > std::chrono::steady_clock::duration::zero()) {
      const auto wait = selected.kind == Job::Kind::Submit
                            ? std::min(delay, selected.enqueued +
                                                  submit_queue_timeout_ - now)
                            : delay;
      lane.schedule_timer.expires_after(wait);
      boost::system::error_code error;
      lane.waiting_for_schedule = true;
      co_await lane.schedule_timer.async_wait(
          boost::asio::redirect_error(boost::asio::use_awaitable, error));
      lane.waiting_for_schedule = false;
      continue;
    }
    Job job = PopNextJob();
    job.dispatched = std::chrono::steady_clock::now();
    if (perf_) job.dispatched_ns = BenchClockNowNs();
    auto response = co_await lane.http.Send(MakeRequest(job));
    const uint64_t response_done_ns = perf_ ? BenchClockNowNs() : 0;
    if (response.ok()) {
      scheduler_->Observe(*response, std::chrono::steady_clock::now());
    }
    if (perf_) {
      if (job.write_start_ns != 0 &&
          (job.kind == Job::Kind::Submit || job.kind == Job::Kind::Cancel)) {
        const bool cancel = job.kind == Job::Kind::Cancel;
        perf_->RecordRestRequest({cancel ? job.cancel.client_order_id.value
                                         : job.submit.client_order_id.value,
                                  cancel, job.enqueued_ns,
                                  job.dispatched_ns,
                                  job.write_start_ns,
                                  job.write_done_ns,
                                  job.posted_ns,
                                  job.queued_before_post,
                                  job.active_lanes_on_post,
                                  job.write_trace.first_write_call_ns,
                                  job.write_trace.first_write_return_ns,
                                  job.write_trace.write_calls,
                                  response_done_ns});
      }
      perf_->Record(job.enqueued, job.dispatched, job.write_begin,
                    job.write_done, std::chrono::steady_clock::now(),
                    job.kind == Job::Kind::Submit,
                    job.kind == Job::Kind::Cancel);
    }
    if (job.kind == Job::Kind::Trades && response.ok() &&
        response->status == 200) {
      auto page = ParseTrades(response->body, job.query);
      if (!page.ok()) {
        job.trades_done(page.status());
        continue;
      }
      uint64_t previous = job.from_trade_id == 0 ? 0 : job.from_trade_id - 1;
      bool ordered = true;
      for (const Fill& fill : *page) {
        if (fill.trade_id <= previous) {
          ordered = false;
          break;
        }
        previous = fill.trade_id;
      }
      if (!ordered || job.trade_pages >= 1000 ||
          (page->size() == 1000 &&
           previous == std::numeric_limits<uint64_t>::max())) {
        job.trades_done(
            absl::DataLossError("trade history pagination did not advance"));
        continue;
      }
      job.accumulated_trades.insert(job.accumulated_trades.end(),
                                    std::make_move_iterator(page->begin()),
                                    std::make_move_iterator(page->end()));
      if (page->size() == 1000) {
        job.from_trade_id = previous + 1;
        ++job.trade_pages;
        queues_[Priority(job.kind)].push_front(std::move(job));
        ++queued_jobs_;
        ++accepted_queued_jobs_;
        continue;
      }
      job.trades_done(std::move(job.accumulated_trades));
      continue;
    }
    Complete(job, std::move(response));
  }
  lane.draining = false;
  if (--active_lanes_ == 0 && rest_stop_processed_) FinishRestStop();
}

void BinanceTradeClient::CompleteTrade(Job& job, TradeResponse response) {
  boost::asio::post(
      io_, [done = std::move(job.done), response = std::move(response)]() mutable {
        done(std::move(response));
      });
}

void BinanceTradeClient::Complete(Job& job,
                                  absl::StatusOr<HttpResponse> response) {
  if (job.kind == Job::Kind::Query || job.kind == Job::Kind::Trades ||
      job.kind == Job::Kind::Balances || job.kind == Job::Kind::OpenOrders ||
      job.kind == Job::Kind::Rules) {
    if (!response.ok() || response->status != 200) {
      const absl::Status error =
          !response.ok() ? response.status()
                         : absl::UnavailableError(ErrorText(*response));
      if (job.kind == Job::Kind::Query) job.query_done(error);
      if (job.kind == Job::Kind::Trades) job.trades_done(error);
      if (job.kind == Job::Kind::Balances) job.balances_done(error);
      if (job.kind == Job::Kind::OpenOrders) job.open_orders_done(error);
      if (job.kind == Job::Kind::Rules) job.rules_done(error);
    } else if (job.kind == Job::Kind::Query) {
      job.query_done(ParseQueryOrder(response->body, job.query));
    } else if (job.kind == Job::Kind::Trades) {
      job.trades_done(ParseTrades(response->body, job.query));
    } else if (job.kind == Job::Kind::Balances) {
      job.balances_done(ParseBalances(response->body));
    } else if (job.kind == Job::Kind::Rules) {
      job.rules_done(ParseBinanceExchangeInfo(response->body));
    } else {
      job.open_orders_done(ParseOpenOrders(response->body));
    }
    return;
  }
  if (!response.ok()) {
    CompleteTrade(job,
                  TradeResponse{
                      job.write_begin == std::chrono::steady_clock::time_point{}
                          ? TradeResponse::Result::NotSent
                          : TradeResponse::Result::Unknown,
                      {}, std::string(response.status().message())});
    return;
  }
  if (response->status >= 500 || response->status == 408 ||
      response->status == 429 || response->status == 418 ||
      ExecutionMayBeUnknown(*response)) {
    CompleteTrade(job, TradeResponse{TradeResponse::Result::Unknown, {},
                                     ErrorText(*response)});
    return;
  }
  if (response->status != 200) {
    const TradeResponse::Result result = job.kind == Job::Kind::Cancel
                                             ? TradeResponse::Result::Unknown
                                             : TradeResponse::Result::Rejected;
    CompleteTrade(job, TradeResponse{result, {}, ErrorText(*response)});
    return;
  }
  simdjson::dom::parser parser;
  auto root = Parse(parser, response->body);
  if (!root.ok()) {
    CompleteTrade(job, TradeResponse{TradeResponse::Result::Unknown, {},
                                     std::string(root.status().message())});
    return;
  }
  auto exchange_id = Number(*root, "orderId");
  if (!exchange_id.ok()) {
    CompleteTrade(job, TradeResponse{TradeResponse::Result::Unknown, {},
                                     std::string(exchange_id.status().message())});
    return;
  }
  if (job.kind == Job::Kind::Submit) {
    auto client = Text(*root, "clientOrderId");
    if (!client.ok() || *client != job.submit.client_order_id.value) {
      CompleteTrade(job,
                    TradeResponse{TradeResponse::Result::Unknown, {},
                                  "submit response client ID mismatch"});
      return;
    }
    CompleteTrade(job,
                  TradeResponse{TradeResponse::Result::Confirmed,
                                ExchangeOrderId{*exchange_id}, {}});
    return;
  }
  auto original = Text(*root, "origClientOrderId");
  auto quantity = Fixed(*root, "origQty");
  auto executed = Fixed(*root, "executedQty");
  if (!original.ok() || *original != job.cancel.client_order_id.value ||
      !quantity.ok() || !executed.ok() || *executed > *quantity) {
    CompleteTrade(job,
                  TradeResponse{TradeResponse::Result::Unknown, {},
                                "cancel response incomplete or mismatched"});
    return;
  }
  CompleteTrade(job,
                TradeResponse{TradeResponse::Result::Confirmed,
                              ExchangeOrderId{*exchange_id}, {},
                              *quantity - *executed});
}

void BinanceTradeClient::Stop() {
  if (stopped_.exchange(true)) return;
  if (private_stream_enabled_) {
    boost::asio::post(private_io_, [this] {
      user_stream_.Stop();
      private_guard_.reset();
    });
  }
  boost::asio::post(rest_io_, [this] {
    for (const auto& lane : rest_lanes_) {
      lane->schedule_timer.cancel();
      lane->http.Cancel();
    }
    while (queued_jobs_ != 0) {
      Job job = PopNextJob();
      if (job.kind == Job::Kind::Query) {
        job.query_done(absl::CancelledError("trade client stopped"));
      } else if (job.kind == Job::Kind::Trades) {
        job.trades_done(absl::CancelledError("trade client stopped"));
      } else if (job.kind == Job::Kind::Balances) {
        job.balances_done(absl::CancelledError("trade client stopped"));
      } else if (job.kind == Job::Kind::OpenOrders) {
        job.open_orders_done(absl::CancelledError("trade client stopped"));
      } else if (job.kind == Job::Kind::Rules) {
        job.rules_done(absl::CancelledError("trade client stopped"));
      } else {
        CompleteTrade(job, TradeResponse{
            TradeResponse::Result::NotSent, {}, "trade client stopped"});
      }
    }
    rest_stop_processed_ = true;
    if (active_lanes_ == 0) FinishRestStop();
  });
}

void BinanceTradeClient::FinishRestStop() {
  if (rest_stop_finished_) return;
  rest_stop_finished_ = true;
  rest_guard_.reset();
  boost::asio::post(io_, [this] { account_guard_.reset(); });
}

}  // namespace hquant::v1
