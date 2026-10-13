#include "hquant/bench/venue/sim_venue.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef __APPLE__
#include <pthread/qos.h>
#endif

#include "boost/asio.hpp"
#include "boost/beast.hpp"
#include "hquant/bench/venue/matching_engine.h"
#include "hquant/venue/binance/auth.h"
#include "simdjson.h"

namespace hquant::bench {
namespace {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;
using Error = boost::system::error_code;

int SetBenchmarkQos(bool requested) {
  if (!requested) return 0;
#ifdef __APPLE__
  return ::pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#else
  return -1;
#endif
}

constexpr int64_t kScale = 1'000'000'000;
constexpr int64_t kCent = 10'000'000;
constexpr std::string_view kKey = "hquant-sim-key";
constexpr std::string_view kSecret = "hquant-sim-secret";

uint64_t ClockNs() {
  timespec t{};
  clock_gettime(CLOCK_MONOTONIC, &t);
  return uint64_t(t.tv_sec) * 1'000'000'000 + t.tv_nsec;
}

uint64_t ReadReadyNs(int fd) {
  using Probe = uint64_t (*)(int);
  static Probe probe = reinterpret_cast<Probe>(
      dlsym(RTLD_DEFAULT, "hquant_kevent_read_ready_ns"));
  return probe ? probe(fd) : 0;
}

uint64_t EpochMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string Dec(int64_t n) {
  return hquant::v1::BinanceDecimal(static_cast<uint64_t>(n));
}

int64_t MulFixed(int64_t lhs, int64_t rhs) {
  return static_cast<int64_t>(static_cast<__int128>(lhs) * rhs / kScale);
}

std::optional<int64_t> Fixed(std::string_view s) {
  if (s.empty()) return std::nullopt;
  int64_t whole = 0, fraction = 0;
  size_t i = 0;
  for (; i < s.size() && s[i] != '.'; ++i) {
    if (s[i] < '0' || s[i] > '9' || whole > 9'000'000'000LL)
      return std::nullopt;
    whole = whole * 10 + (s[i] - '0');
  }
  if (whole > 9'000'000'000LL) return std::nullopt;
  int digits = 0;
  if (i < s.size()) {
    ++i;
    for (; i < s.size(); ++i) {
      if (s[i] < '0' || s[i] > '9' || ++digits > 9) return std::nullopt;
      fraction = fraction * 10 + (s[i] - '0');
    }
  }
  while (digits++ < 9) fraction *= 10;
  return whole * kScale + fraction;
}

std::optional<uint64_t> Unsigned(std::string_view text) {
  if (text.empty()) return std::nullopt;
  uint64_t value = 0;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size())
    return std::nullopt;
  return value;
}

std::string Param(std::string_view query, std::string_view key) {
  size_t start = 0;
  while (start < query.size()) {
    size_t end = query.find('&', start);
    if (end == std::string_view::npos) end = query.size();
    const auto part = query.substr(start, end - start);
    if (part.substr(0, key.size()) == key && part.size() > key.size() &&
        part[key.size()] == '=')
      return std::string(part.substr(key.size() + 1));
    start = end + 1;
  }
  return {};
}

std::string JsonError(int code, std::string_view message) {
  return "{\"code\":" + std::to_string(code) + ",\"msg\":\"" +
         std::string(message) + "\"}";
}

std::string SymbolName(size_t shard) {
  char buffer[32];
  std::snprintf(buffer, sizeof(buffer), "STRESSSIM%04zu", shard + 1);
  return buffer;
}

std::string Status(const SimOrder& order) {
  if (order.canceled) return "CANCELED";
  if (order.remaining_nanos == 0) return "FILLED";
  if (order.filled_nanos > 0) return "PARTIALLY_FILLED";
  return "NEW";
}

struct Balance {
  int64_t free = 0;
  int64_t locked = 0;
};

struct AccountOrder {
  std::string client;
  std::string symbol;
  Side side = Side::Buy;
  int64_t price = 0;
  int64_t quantity = 0;
  int64_t lock = 0;
  int64_t reported_filled = 0;
  uint64_t id = 0;
  uint64_t update_ms = 0;
};

struct AccountTrade {
  uint64_t id = 0;
  uint64_t order_id = 0;
  std::string client;
  std::string symbol;
  Side side = Side::Buy;
  int64_t price = 0;
  int64_t quantity = 0;
  int64_t fee = 0;
  uint64_t time_ms = 0;
};

std::string SideText(Side side) { return side == Side::Buy ? "BUY" : "SELL"; }

struct QueuedFrame {
  std::string body;
  uint64_t bench_seq = 0;
  std::string client;
  std::string status;
  uint64_t exchange_id = 0;
  uint64_t trade_id = 0;
};
}  // namespace

struct SimVenue::Impl {
  struct WsSession;
  struct HttpSession;
  struct Listener;

  explicit Impl(size_t count)
      : count(count), timer(io), guard(net::make_work_guard(io)),
        rest_guard(net::make_work_guard(rest_io)) {
    if (count == 0 || count > 1000)
      throw std::invalid_argument("symbols must be 1..1000");
    symbols.reserve(count);
    books.resize(count);
    for (size_t shard = 0; shard < count; ++shard) {
      const std::string symbol = SymbolName(shard);
      symbols.push_back(symbol);
      by_symbol.emplace(symbol, shard);
      wallet.emplace(symbol, Balance{1000 * kScale, 0});
      auto& b = books[shard];
      b.sequence = 9;
      const int64_t mid = (10'000 + static_cast<int64_t>(shard) * 5) * kCent;
      b.bid = mid - kCent;
      b.ask = mid + kCent;
      for (int level = 0; level < 10; ++level) {
        const int64_t quantity =
            (level == 0 && shard % 10 == 0 ? 1 : 1000) * kScale;
        const auto bid = matcher.Place(symbol, Side::Buy, Owner::External,
                                       b.bid - level * kCent, quantity);
        const auto ask = matcher.Place(symbol, Side::Sell, Owner::External,
                                       b.ask + level * kCent, quantity);
        if (level == 0) {
          b.seed_bid = bid.order_id;
          b.seed_ask = ask.order_id;
        }
      }
    }
    wallet.emplace("USD", Balance{1'000'000'000LL * kScale, 0});
    listeners.emplace_back(std::make_unique<Listener>(*this, 0));
    listeners.emplace_back(std::make_unique<Listener>(*this, 1));
    listeners.emplace_back(std::make_unique<Listener>(*this, 2));
    listeners.emplace_back(std::make_unique<Listener>(*this, 3));
    ports.public_rest = listeners[0]->port();
    ports.public_ws = listeners[1]->port();
    ports.trade_rest = listeners[2]->port();
    ports.private_ws = listeners[3]->port();
    trace.market_send_ns.push_back(0);  // bench_seq starts at one.
  }

  struct BookState {
    uint64_t sequence = 9;
    uint64_t trade_id = 0;
    int64_t bid = 0;
    int64_t ask = 0;
    uint64_t seed_bid = 0;
    uint64_t seed_ask = 0;
    uint64_t deep_bid = 0;
    uint64_t deep_ask = 0;
  };

  struct WsSession : std::enable_shared_from_this<WsSession> {
    Impl& venue;
    websocket::stream<tcp::socket> ws;
    beast::flat_buffer buffer;
    std::deque<QueuedFrame> pending;
    bool writing = false;
    bool private_stream = false;
    bool subscribed = false;
    size_t shard = 0;

    WsSession(Impl& venue, tcp::socket socket, bool priv)
        : venue(venue), ws(std::move(socket)), private_stream(priv) {}

    void Accept(http::request<http::string_body> request) {
      std::string target(request.target());
      if (!private_stream) {
        const size_t at = target.find("?streams=");
        if (target.substr(0, at) != "/stream" || at == std::string::npos)
          return;
        std::string stream = target.substr(at + 9);
        const size_t depth = stream.find("@depth/");
        if (depth == std::string::npos) return;
        std::string lower = stream.substr(0, depth);
        if (stream.substr(depth + 7) != lower + "@trade") return;
        std::transform(lower.begin(), lower.end(), lower.begin(), ::toupper);
        const auto it = venue.by_symbol.find(lower);
        if (it == venue.by_symbol.end()) return;
        shard = it->second;
      } else if (target != "/ws-api/v3") {
        return;
      }
      auto self = shared_from_this();
      ws.async_accept(request, [self](Error error) {
        if (error) return;
        if (!self->private_stream) {
          self->venue.public_clients[self->shard] = self;
          ++self->venue.public_ready;
          self->Send({self->venue.DepthFrame(self->shard, {}, {}, 0, true)});
        }
        self->Read();
      });
    }

    void Read() {
      auto self = shared_from_this();
      ws.async_read(buffer, [self](Error error, size_t) {
        if (error) {
          self->Close();
          return;
        }
        if (self->private_stream && !self->subscribed) {
          std::string body = beast::buffers_to_string(self->buffer.data());
          simdjson::dom::parser parser;
          simdjson::dom::element root;
          uint64_t id = 0;
          bool valid = false;
          if (!parser.parse(body).get(root)) {
            (void)root["id"].get(id);
            std::string_view method, api, signature;
            uint64_t timestamp = 0;
            simdjson::dom::element params;
            if (!root["method"].get(method) && !root["params"].get(params) &&
                !params["apiKey"].get(api) &&
                !params["timestamp"].get(timestamp) &&
                !params["signature"].get(signature)) {
              const std::string payload =
                  "apiKey=" + std::string(api) +
                  "&timestamp=" + std::to_string(timestamp);
              valid = method == "userDataStream.subscribe.signature" &&
                      api == kKey &&
                      signature ==
                          hquant::v1::BinanceHmacSignature(kSecret, payload);
            }
          }
          self->Send({"{\"id\":" + std::to_string(id) +
                      ",\"status\":" + (valid ? "200" : "400") +
                      ",\"result\":{\"subscriptionId\":0}}"});
          if (valid) {
            self->subscribed = true;
            self->venue.private_client = self;
            self->venue.private_ready = true;
            self->venue.SendBalancesAll();
          }
        }
        self->buffer.consume(self->buffer.size());
        self->Read();
      });
    }

    void Send(QueuedFrame frame) {
      if (!frame.client.empty()) ++venue.pending_private_reports;
      pending.push_back(std::move(frame));
      if (!writing) Write();
    }

    void Write() {
      if (pending.empty()) {
        writing = false;
        return;
      }
      writing = true;
      auto& frame = pending.front();
      const uint64_t now = ClockNs();
      if (frame.bench_seq &&
          frame.bench_seq < venue.trace.market_send_ns.size())
        venue.trace.market_send_ns[frame.bench_seq] = now;
      const size_t report_index = venue.trace.reports.size();
      if (!frame.client.empty()) {
        venue.trace.reports.push_back({frame.client, frame.status, now,
                                       frame.exchange_id, frame.trade_id});
        ++venue.trace.private_reports;
        ++venue.started_private_reports;
      }
      auto self = shared_from_this();
      const bool report = !frame.client.empty();
      ws.text(true);
      ws.async_write(net::buffer(frame.body),
                     [self, report, report_index](Error error, size_t) {
                       if (error) {
                         self->Close();
                         return;
                       }
                       if (report) {
                         self->venue.trace.reports[report_index].write_done_ns =
                             ClockNs();
                         --self->venue.pending_private_reports;
                       }
                       self->pending.pop_front();
                       self->Write();
                     });
    }

    void Close() {
      if (private_stream && venue.private_client.lock().get() == this) {
        venue.private_client.reset();
        venue.private_ready = false;
      }
      if (!private_stream && venue.public_clients[shard].lock().get() == this) {
        venue.public_clients.erase(shard);
        --venue.public_ready;
      }
      Error ignored;
      ws.next_layer().close(ignored);
    }
  };

  struct HttpSession : std::enable_shared_from_this<HttpSession> {
    struct TimedReadStream {
      using executor_type = tcp::socket::executor_type;
      tcp::socket& socket;
      uint64_t& first_read_ns;
      uint64_t& last_read_ns;
      uint32_t& reads;
      uint64_t& read_ready_ns;

      executor_type get_executor() const noexcept {
        return socket.get_executor();
      }

      template <typename Buffers, typename Handler>
      void async_read_some(const Buffers& buffers, Handler&& handler) {
        socket.async_read_some(
            buffers,
            [this, handler = std::forward<Handler>(handler)](
                Error error, size_t bytes) mutable {
              if (bytes != 0) {
                const uint64_t now = ClockNs();
                if (first_read_ns == 0) {
                  first_read_ns = now;
                  read_ready_ns = ReadReadyNs(socket.native_handle());
                }
                last_read_ns = now;
                ++reads;
              }
              handler(error, bytes);
            });
      }
    };

    Impl& venue;
    tcp::socket socket;
    beast::flat_buffer buffer;
    http::request<http::string_body> request;
    int kind;
    uint64_t first_socket_read_ns = 0;
    uint64_t last_socket_read_ns = 0;
    uint32_t socket_reads = 0;
    uint64_t read_ready_ns = 0;
    TimedReadStream timed_read;

    HttpSession(Impl& venue, tcp::socket socket, int kind)
        : venue(venue), socket(std::move(socket)), kind(kind),
          timed_read{this->socket, first_socket_read_ns,
                     last_socket_read_ns, socket_reads, read_ready_ns} {}

    void Read() {
      auto self = shared_from_this();
      request = {};
      first_socket_read_ns = 0;
      last_socket_read_ns = 0;
      socket_reads = 0;
      read_ready_ns = 0;
      auto complete = [self](Error error, size_t) {
        if (error) return;
        const uint64_t received_ns = ClockNs();
        if (self->kind == 2) {
          net::post(self->venue.io, [self, received_ns] {
            self->HandleRequest(received_ns);
          });
        } else {
          self->HandleRequest(received_ns);
        }
      };
      if (kind == 2)
        http::async_read(timed_read, buffer, request, std::move(complete));
      else
        http::async_read(socket, buffer, request, std::move(complete));
    }

    void HandleRequest(uint64_t received_ns) {
      if (kind == 2) {
        const std::string_view target(request.target().data(),
                                      request.target().size());
        constexpr std::string_view order_path = "/api/v3/order";
        const auto method = request.method();
        if ((method == http::verb::post || method == http::verb::delete_) &&
            target.starts_with(order_path) &&
            target.size() > order_path.size() &&
            target[order_path.size()] == '?') {
          const std::string client = Param(
              target.substr(order_path.size() + 1),
              method == http::verb::post ? "newClientOrderId"
                                         : "origClientOrderId");
          if (!client.empty()) {
            VenueTrace::AccountStamp stamp;
            stamp.client_id = client;
            stamp.status = method == http::verb::post ? "SUBMIT" : "CANCEL";
            stamp.at_ns = received_ns;
            stamp.first_socket_read_ns = first_socket_read_ns;
            stamp.last_socket_read_ns = last_socket_read_ns;
            stamp.socket_reads = socket_reads;
            stamp.read_ready_ns = read_ready_ns;
            venue.trace.requests.push_back(std::move(stamp));
          }
        }
      }
      if (kind == 1 || kind == 3) {
        auto ws = std::make_shared<WsSession>(venue, std::move(socket),
                                              kind == 3);
        ws->Accept(std::move(request));
        return;
      }
      auto [code, body] = venue.Route(kind, request);
      auto response = std::make_shared<http::response<http::string_body>>(
          static_cast<http::status>(code), request.version());
      response->set(http::field::server, "hquant-cpp-sim");
      response->set(http::field::content_type, "application/json");
      response->keep_alive(request.keep_alive());
      response->body() = std::move(body);
      response->prepare_payload();
      auto self = shared_from_this();
      auto write_response = [self, response] {
        http::async_write(self->socket, *response,
                          [self, response](Error error, size_t) {
                            if (!error && response->keep_alive()) self->Read();
                          });
      };
      if (kind == 2)
        net::post(socket.get_executor(), std::move(write_response));
      else
        write_response();
    }
  };

  struct Listener {
    Impl& venue;
    tcp::acceptor acceptor;
    int kind;

    Listener(Impl& venue, int kind)
        : venue(venue),
          acceptor(kind == 2 ? venue.rest_io : venue.io), kind(kind) {
      acceptor.open(tcp::v4());
      acceptor.set_option(net::socket_base::reuse_address(true));
      acceptor.bind({net::ip::make_address_v4("127.0.0.1"), 0});
      acceptor.listen(net::socket_base::max_listen_connections);
    }

    uint16_t port() const { return acceptor.local_endpoint().port(); }

    void Accept() {
      acceptor.async_accept([this](Error error, tcp::socket socket) {
        if (!error)
          std::make_shared<HttpSession>(venue, std::move(socket), kind)->Read();
        if (acceptor.is_open()) Accept();
      });
    }
  };

  using Levels = std::vector<SimLevel>;
  std::string LevelRows(const Levels& levels) const {
    std::string out = "[";
    bool first = true;
    for (const auto& level : levels) {
      if (!first) out += ',';
      first = false;
      out += "[\"" + Dec(level.price_nanos) + "\",\"" +
             Dec(level.quantity_nanos) + "\"]";
    }
    out += ']';
    return out;
  }

  std::string DepthFrame(size_t shard, const Levels& old_bids,
                         const Levels& old_asks, uint64_t seq,
                         bool bootstrap = false) {
    const auto& name = symbols[shard];
    auto bids = matcher.Levels(name, Side::Buy);
    auto asks = matcher.Levels(name, Side::Sell);
    auto tombstones = [](Levels& now, const Levels& previous) {
      for (const auto& prior : previous) {
        if (std::none_of(now.begin(), now.end(), [&](const SimLevel& level) {
              return level.price_nanos == prior.price_nanos;
            }))
          now.push_back({prior.price_nanos, 0});
      }
    };
    if (!bootstrap) {
      tombstones(bids, old_bids);
      tombstones(asks, old_asks);
    }
    const uint64_t update = ++books[shard].sequence;
    std::string body = "{\"e\":\"depthUpdate\",\"s\":\"" + name +
                       "\",\"U\":" + std::to_string(update) +
                       ",\"u\":" + std::to_string(update) +
                       ",\"b\":" + LevelRows(bids) +
                       ",\"a\":" + LevelRows(asks);
    if (seq) body += ",\"bench_seq\":" + std::to_string(seq);
    return body + "}";
  }

  std::string TradeFrame(size_t shard, const SimFill& fill, Side taker_side,
                         uint64_t seq) {
    std::string body =
        "{\"e\":\"trade\",\"s\":\"" + symbols[shard] +
        "\",\"t\":" + std::to_string(++books[shard].trade_id) + ",\"p\":\"" +
        Dec(fill.price_nanos) + "\",\"q\":\"" + Dec(fill.quantity_nanos) +
        "\",\"m\":" + (taker_side == Side::Sell ? "true" : "false");
    if (seq) body += ",\"bench_seq\":" + std::to_string(seq);
    return body + "}";
  }

  uint64_t NextSeq() {
    trace.market_send_ns.push_back(0);
    ++trace.market_messages;
    emitted.store(trace.market_messages, std::memory_order_release);
    return trace.market_messages;
  }

  void SendPublic(size_t shard, std::string body, uint64_t seq) {
    if (auto ws = public_clients[shard].lock())
      ws->Send({std::move(body), seq});
  }

  void SendPrivate(std::string body, const std::string& client = {},
                   const std::string& status = {}, uint64_t id = 0,
                   uint64_t trade_id = 0) {
    if (auto ws = private_client.lock()) {
      ws->Send({"{\"subscriptionId\":0,\"event\":" + body + "}", 0, client,
                status, id, trade_id});
    }
  }

  std::string OrderJson(const AccountOrder& account, bool cancel = false) {
    const SimOrder* order = matcher.Find(account.id);
    const std::string status = order ? Status(*order) : "NEW";
    const int64_t filled = order ? order->filled_nanos : 0;
    std::string body =
        "{\"symbol\":\"" + account.symbol +
        "\",\"orderId\":" + std::to_string(account.id) +
        ",\"clientOrderId\":\"" + account.client + "\",\"price\":\"" +
        Dec(account.price) + "\",\"origQty\":\"" + Dec(account.quantity) +
        "\",\"executedQty\":\"" + Dec(filled) + "\",\"status\":\"" + status +
        "\",\"side\":\"" + SideText(account.side) +
        "\",\"updateTime\":" + std::to_string(account.update_ms);
    if (cancel) body += ",\"origClientOrderId\":\"" + account.client + "\"";
    return body + "}";
  }

  void Report(AccountOrder& account, std::string_view execution,
              int64_t last_qty = 0, int64_t last_price = 0, int64_t fee = 0,
              uint64_t trade_id = 0) {
    account.update_ms = ++event_ms;
    const std::string status = execution == "NEW"        ? "NEW"
                               : execution == "CANCELED" ? "CANCELED"
                               : account.reported_filled == account.quantity
                                   ? "FILLED"
                                   : "PARTIALLY_FILLED";
    SendPrivate("{\"e\":\"executionReport\",\"E\":" + std::to_string(event_ms) +
                    ",\"T\":" + std::to_string(event_ms) + ",\"s\":\"" +
                    account.symbol + "\",\"c\":\"" + account.client +
                    "\",\"S\":\"" + SideText(account.side) +
                    "\",\"o\":\"LIMIT\",\"f\":\"GTC\",\"q\":\"" +
                    Dec(account.quantity) + "\",\"p\":\"" + Dec(account.price) +
                    "\",\"C\":\"\",\"x\":\"" + std::string(execution) +
                    "\",\"X\":\"" + status + "\",\"r\":\"NONE\",\"i\":" +
                    std::to_string(account.id) + ",\"l\":\"" + Dec(last_qty) +
                    "\",\"z\":\"" + Dec(account.reported_filled) +
                    "\",\"L\":\"" + Dec(last_price) + "\",\"n\":\"" + Dec(fee) +
                    "\",\"N\":\"USD\",\"t\":" + std::to_string(trade_id) +
                    ",\"Y\":\"" + Dec(MulFixed(last_price, last_qty)) + "\"}",
                account.client, status, account.id, trade_id);
  }

  void SendBalances(const std::vector<std::string>& assets) {
    ++event_ms;
    std::string body =
        "{\"e\":\"outboundAccountPosition\",\"E\":" + std::to_string(event_ms) +
        ",\"u\":" + std::to_string(event_ms) + ",\"B\":[";
    bool first = true;
    for (const auto& asset : assets) {
      const Balance& balance = wallet.at(asset);
      if (!first) body += ',';
      first = false;
      body += "{\"a\":\"" + asset + "\",\"f\":\"" + Dec(balance.free) +
              "\",\"l\":\"" + Dec(balance.locked) + "\"}";
    }
    SendPrivate(body + "]}");
  }

  void SendBalancesAll() {
    std::vector<std::string> assets;
    assets.reserve(wallet.size());
    for (const auto& [name, value] : wallet) assets.push_back(name);
    SendBalances(assets);
  }

  void Settle(uint64_t id, const SimFill& fill, uint64_t action_ns,
              uint64_t market_seq) {
    auto it = client_by_id.find(id);
    if (it == client_by_id.end()) return;
    AccountOrder& account = orders.at(it->second);
    Balance& base = wallet.at(account.symbol);
    Balance& quote = wallet.at("USD");
    const int64_t gross = MulFixed(fill.price_nanos, fill.quantity_nanos);
    const int64_t fee = (gross + 999) / 1000;
    if (account.side == Side::Buy) {
      const int64_t reserved = MulFixed(account.price, fill.quantity_nanos);
      account.lock -= reserved;
      quote.locked -= reserved;
      quote.free += reserved - gross - fee;
      base.free += fill.quantity_nanos;
    } else {
      account.lock -= fill.quantity_nanos;
      base.locked -= fill.quantity_nanos;
      quote.free += gross - fee;
    }
    const uint64_t trade_id = ++next_account_trade_id;
    trace.matches.push_back({trade_id, action_ns, market_seq});
    account.reported_filled += fill.quantity_nanos;
    Report(account, "TRADE", fill.quantity_nanos, fill.price_nanos, fee,
           trade_id);
    trades.push_back({trade_id, id, account.client, account.symbol,
                      account.side, fill.price_nanos, fill.quantity_nanos, fee,
                      account.update_ms});
    SendBalances({account.symbol, "USD"});
    ++trace.account_fills;
  }

  void ApplyPlace(size_t shard, Side side, Owner owner, int64_t price,
                  int64_t quantity, AccountOrder* account = nullptr,
                  bool tagged = false) {
    const uint64_t action_ns = ClockNs();
    const std::string& symbol = symbols[shard];
    const auto old_bids = matcher.Levels(symbol, Side::Buy);
    const auto old_asks = matcher.Levels(symbol, Side::Sell);
    const auto result = matcher.Place(symbol, side, owner, price, quantity);
    if (tagged && owner == Owner::External && result.remaining_nanos != 0)
      throw std::runtime_error(
          "external sweep left an unintended resting order");
    if (account) {
      account->id = result.order_id;
      client_by_id.emplace(result.order_id, account->client);
      Report(*account, "NEW");
    }
    for (const auto& fill : result.fills) {
      const uint64_t seq = tagged ? NextSeq() : 0;
      SendPublic(shard, TradeFrame(shard, fill, side, seq), seq);
      if (fill.maker_owner == Owner::Account)
        Settle(fill.maker_id, fill, action_ns, seq);
      if (fill.taker_owner == Owner::Account)
        Settle(fill.taker_id, fill, action_ns, seq);
    }
    const uint64_t seq = tagged ? NextSeq() : 0;
    SendPublic(shard, DepthFrame(shard, old_bids, old_asks, seq), seq);
    ++trace.market_actions;
  }

  bool ApplyCancel(size_t shard, uint64_t id, bool tagged = false) {
    const std::string& symbol = symbols[shard];
    const auto old_bids = matcher.Levels(symbol, Side::Buy);
    const auto old_asks = matcher.Levels(symbol, Side::Sell);
    auto released = matcher.Cancel(id);
    if (!released) return false;
    const uint64_t seq = tagged ? NextSeq() : 0;
    SendPublic(shard, DepthFrame(shard, old_bids, old_asks, seq), seq);
    ++trace.market_actions;
    return true;
  }

  std::string Snapshot(size_t shard) {
    return "{\"lastUpdateId\":" + std::to_string(books[shard].sequence) +
           ",\"bids\":" + LevelRows(matcher.Levels(symbols[shard], Side::Buy)) +
           ",\"asks\":" +
           LevelRows(matcher.Levels(symbols[shard], Side::Sell)) + "}";
  }

  bool Signed(const http::request<http::string_body>& request,
              std::string_view query) const {
    auto header = request.find("X-MBX-APIKEY");
    if (header == request.end() || header->value() != kKey) return false;
    const size_t at = query.rfind("&signature=");
    if (at == std::string_view::npos) return false;
    return query.substr(at + 11) ==
           hquant::v1::BinanceHmacSignature(kSecret, query.substr(0, at));
  }

  std::string ExchangeInfo() {
    std::string body = "{\"rateLimits\":[],\"symbols\":[";
    for (size_t i = 0; i < count; ++i) {
      if (i) body += ',';
      body += "{\"symbol\":\"" + symbols[i] +
              "\",\"status\":\"TRADING\",\"baseAsset\":\"" + symbols[i] +
              "\",\"quoteAsset\":\"USD\",\"filters\":["
              "{\"filterType\":\"PRICE_FILTER\",\"minPrice\":\"0\","
              "\"maxPrice\":\"0\",\"tickSize\":\"0.01\"},"
              "{\"filterType\":\"LOT_SIZE\",\"minQty\":\"1\","
              "\"maxQty\":\"1000000000\",\"stepSize\":\"1\"},"
              "{\"filterType\":\"MIN_NOTIONAL\",\"minNotional\":\"1\"}]}";
    }
    return body + "]}";
  }

  std::pair<int, std::string> Route(
      int kind, const http::request<http::string_body>& request) {
    const std::string target(request.target());
    const size_t qmark = target.find('?');
    const std::string path = target.substr(0, qmark);
    const std::string_view query =
        qmark == std::string::npos ? std::string_view{}
                                   : std::string_view(target).substr(qmark + 1);
    if (kind == 0) {
      const std::string symbol = Param(query, "symbol");
      const auto it = by_symbol.find(symbol);
      if (request.method() == http::verb::get && path == "/api/v3/depth" &&
          it != by_symbol.end())
        return {200, Snapshot(it->second)};
      return {404, JsonError(-1000, "Unknown public request")};
    }
    if (request.method() == http::verb::get && path == "/api/v3/exchangeInfo")
      return {200, ExchangeInfo()};
    if (!Signed(request, query))
      return {401, JsonError(-1022, "Bad signature")};
    if (request.method() == http::verb::get && path == "/api/v3/account") {
      std::string body = "{\"balances\":[";
      bool first = true;
      for (const auto& [asset, balance] : wallet) {
        if (!first) body += ',';
        first = false;
        body += "{\"asset\":\"" + asset + "\",\"free\":\"" + Dec(balance.free) +
                "\",\"locked\":\"" + Dec(balance.locked) + "\"}";
      }
      return {200, body + "]}"};
    }
    if (request.method() == http::verb::get && path == "/api/v3/openOrders") {
      std::string body = "[";
      bool first = true;
      for (auto& [client, order] : orders) {
        const std::string status = Status(*matcher.Find(order.id));
        if (status != "NEW" && status != "PARTIALLY_FILLED") continue;
        if (!first) body += ',';
        first = false;
        body += OrderJson(order);
      }
      return {200, body + "]"};
    }
    if (request.method() == http::verb::post && path == "/api/v3/order") {
      const std::string symbol = Param(query, "symbol");
      const std::string client = Param(query, "newClientOrderId");
      const std::string side_text = Param(query, "side");
      const auto price = Fixed(Param(query, "price"));
      const auto qty = Fixed(Param(query, "quantity"));
      const __int128 gross =
          price && qty ? static_cast<__int128>(*price) * (*qty / kScale) : 0;
      const auto at = by_symbol.find(symbol);
      if (at == by_symbol.end() || client.empty() || orders.count(client) ||
          (side_text != "BUY" && side_text != "SELL") || !price || !qty ||
          *price <= 0 || *qty <= 0 || *price % kCent || *qty % kScale ||
          gross < kScale || gross > std::numeric_limits<int64_t>::max() ||
          Param(query, "type") != "LIMIT" ||
          Param(query, "timeInForce") != "GTC")
        return {400, JsonError(-1013, "Invalid order or filter")};
      const Side side = side_text == "BUY" ? Side::Buy : Side::Sell;
      const int64_t lock =
          side == Side::Buy ? static_cast<int64_t>(gross) : *qty;
      Balance& balance = wallet.at(side == Side::Buy ? "USD" : symbol);
      ++trace.account_submits;
      if (balance.free < lock)
        return {400, JsonError(-2010, "Account has insufficient balance")};
      balance.free -= lock;
      balance.locked += lock;
      auto [it, inserted] = orders.emplace(
          client, AccountOrder{client, symbol, side, *price, *qty, lock});
      (void)inserted;
      ApplyPlace(at->second, side, Owner::Account, *price, *qty, &it->second);
      SendBalances({side == Side::Buy ? "USD" : symbol});
      return {200, OrderJson(it->second)};
    }
    if (path == "/api/v3/order") {
      const std::string symbol = Param(query, "symbol");
      const std::string client = Param(query, "origClientOrderId");
      auto it = orders.find(client);
      if (it == orders.end() || it->second.symbol != symbol)
        return {400, JsonError(-2011, "Unknown order sent")};
      if (request.method() == http::verb::get)
        return {200, OrderJson(it->second)};
      if (request.method() == http::verb::delete_) {
        ++trace.account_cancels;
        AccountOrder& order = it->second;
        if (!ApplyCancel(by_symbol.at(symbol), order.id))
          return {400, JsonError(-2011, "Unknown order sent")};
        Balance& balance = wallet.at(order.side == Side::Buy ? "USD" : symbol);
        balance.locked -= order.lock;
        balance.free += order.lock;
        order.lock = 0;
        Report(order, "CANCELED");
        SendBalances({order.side == Side::Buy ? "USD" : symbol});
        return {200, OrderJson(order, true)};
      }
    }
    if (request.method() == http::verb::get && path == "/api/v3/myTrades") {
      const std::string symbol = Param(query, "symbol");
      const std::string order_id = Param(query, "orderId");
      const std::string cursor = Param(query, "fromId");
      const std::string limit_text = Param(query, "limit");
      const auto id = Unsigned(order_id);
      const auto from =
          cursor.empty() ? std::optional<uint64_t>(0) : Unsigned(cursor);
      const auto limit = limit_text.empty() ? std::optional<uint64_t>(500)
                                            : Unsigned(limit_text);
      if (!id || !from || !limit || *limit == 0 || *limit > 1000)
        return {400, JsonError(-1102, "Invalid trade cursor")};
      std::string body = "[";
      bool first = true;
      int n = 0;
      for (const auto& trade : trades) {
        if (trade.symbol != symbol || trade.order_id != *id || trade.id < *from)
          continue;
        if (n++ >= *limit) break;
        if (!first) body += ',';
        first = false;
        body += "{\"symbol\":\"" + symbol +
                "\",\"id\":" + std::to_string(trade.id) +
                ",\"orderId\":" + std::to_string(*id) + ",\"price\":\"" +
                Dec(trade.price) + "\",\"qty\":\"" + Dec(trade.quantity) +
                "\",\"quoteQty\":\"" +
                Dec(MulFixed(trade.price, trade.quantity)) +
                "\",\"commission\":\"" + Dec(trade.fee) +
                "\",\"commissionAsset\":\"USD\",\"time\":" +
                std::to_string(trade.time_ms) +
                ",\"isBuyer\":" + (trade.side == Side::Buy ? "true" : "false") +
                "}";
      }
      return {200, body + "]"};
    }
    return {404, JsonError(-1000, "Unknown request")};
  }

  void Tick() {
    if (next_action >= total_actions) {
      replay_done = true;
      return;
    }
    const size_t action = next_action++;
    const size_t shard = action % count;
    const size_t slot = action / count;
    auto& book = books[shard];
    if (slot % 5 == 4) {
      const Side taker = (slot / 5) % 2 == 0 ? Side::Sell : Side::Buy;
      const int64_t bbo = taker == Side::Sell ? book.bid : book.ask;
      // Cross the next nine levels too, so an external order never leaves an
      // accidental resting remainder after consuming the BBO. Fill symbols
      // consume one queued external lot and one account lot per sweep.
      const int64_t marketable_limit =
          taker == Side::Sell ? bbo - 9 * kCent : bbo + 9 * kCent;
      const int64_t quantity = (shard % 10 == 0 ? 2 : 3) * kScale;
      ApplyPlace(shard, taker, Owner::External, marketable_limit, quantity,
                 nullptr, true);
      // Restore a depleted BBO. A large external queue is left unchanged.
      const Side maker = taker == Side::Sell ? Side::Buy : Side::Sell;
      const auto levels = matcher.Levels(symbols[shard], maker);
      if (levels.empty() || levels.front().price_nanos != bbo)
        ApplyPlace(shard, maker, Owner::External, bbo, kScale);
    } else {
      const bool bid = slot % 2 == 0;
      const int64_t price = (bid ? book.bid - 8 * kCent : book.ask + 8 * kCent);
      uint64_t& id = bid ? book.deep_bid : book.deep_ask;
      if (id && matcher.Find(id)->remaining_nanos > 0) {
        ApplyCancel(shard, id, true);
        id = 0;
      } else {
        const auto old_bids = matcher.Levels(symbols[shard], Side::Buy);
        const auto old_asks = matcher.Levels(symbols[shard], Side::Sell);
        auto result =
            matcher.Place(symbols[shard], bid ? Side::Buy : Side::Sell,
                          Owner::External, price, kScale);
        id = result.order_id;
        const uint64_t seq = NextSeq();
        SendPublic(shard, DepthFrame(shard, old_bids, old_asks, seq), seq);
        ++trace.market_actions;
      }
    }
    const auto deadline =
        replay_start +
        std::chrono::nanoseconds((next_action * duration_ns) / total_actions);
    timer.expires_at(deadline);
    timer.async_wait([this](Error error) {
      if (!error) Tick();
    });
  }

  size_t count;
  net::io_context io;
  net::io_context rest_io;
  net::steady_timer timer;
  net::executor_work_guard<net::io_context::executor_type> guard;
  net::executor_work_guard<net::io_context::executor_type> rest_guard;
  std::thread worker;
  std::thread rest_worker;
  std::atomic<int> market_io_qos_status{0};
  std::atomic<int> rest_io_qos_status{0};
  VenuePorts ports;
  MatchingEngine matcher;
  std::vector<std::string> symbols;
  std::unordered_map<std::string, size_t> by_symbol;
  std::vector<BookState> books;
  std::vector<std::unique_ptr<Listener>> listeners;
  std::map<size_t, std::weak_ptr<WsSession>> public_clients;
  std::weak_ptr<WsSession> private_client;
  std::unordered_map<std::string, Balance> wallet;
  std::unordered_map<std::string, AccountOrder> orders;
  std::unordered_map<uint64_t, std::string> client_by_id;
  std::vector<AccountTrade> trades;
  uint64_t next_account_trade_id = 0;
  uint64_t event_ms = EpochMs();
  std::atomic<size_t> public_ready{0};
  std::atomic<bool> private_ready{false};
  std::atomic<bool> replay_done{false};
  std::atomic<uint64_t> emitted{0};
  std::atomic<uint64_t> started_private_reports{0};
  std::atomic<uint64_t> pending_private_reports{0};
  VenueTrace trace;
  std::chrono::steady_clock::time_point replay_start;
  uint64_t duration_ns = 0;
  size_t total_actions = 0;
  size_t next_action = 0;
};

SimVenue::SimVenue(size_t symbols) : impl_(std::make_unique<Impl>(symbols)) {}
SimVenue::~SimVenue() {
  if (impl_ && (impl_->worker.joinable() || impl_->rest_worker.joinable()))
    StopAndCollect();
}
VenuePorts SimVenue::ports() const { return impl_->ports; }
std::string SimVenue::Symbol(size_t shard) { return SymbolName(shard); }
void SimVenue::Start() {
  for (auto& listener : impl_->listeners) listener->Accept();
  const char* qos_env = std::getenv("HQUANT_BENCH_PERFORMANCE_QOS");
  const bool qos_requested = !qos_env || std::strcmp(qos_env, "0") != 0;
  impl_->trace.performance_qos_requested = qos_requested;
  impl_->worker = std::thread([this, qos_requested] {
    impl_->market_io_qos_status = SetBenchmarkQos(qos_requested);
    impl_->io.run();
  });
  try {
    impl_->rest_worker = std::thread([this, qos_requested] {
      impl_->rest_io_qos_status = SetBenchmarkQos(qos_requested);
      impl_->rest_io.run();
    });
  } catch (...) {
    impl_->io.stop();
    impl_->worker.join();
    throw;
  }
}
bool SimVenue::Ready() const {
  return impl_->public_ready == impl_->count && impl_->private_ready;
}
void SimVenue::BeginReplay(uint32_t seconds) {
  net::post(impl_->io, [this, seconds] {
    impl_->replay_start = std::chrono::steady_clock::now();
    impl_->duration_ns = uint64_t(seconds) * 1'000'000'000;
    impl_->total_actions = uint64_t(seconds) * 1000 * impl_->count / 120;
    impl_->next_action = 0;
    impl_->Tick();
  });
}
bool SimVenue::ReplayDone() const { return impl_->replay_done; }
uint64_t SimVenue::MarketMessageCount() const { return impl_->emitted; }
uint64_t SimVenue::PrivateReportCount() const {
  return impl_->started_private_reports;
}
uint64_t SimVenue::PendingPrivateReports() const {
  return impl_->pending_private_reports;
}
VenueTrace SimVenue::StopAndCollect() {
  impl_->rest_io.stop();
  impl_->io.stop();
  if (impl_->worker.joinable()) impl_->worker.join();
  if (impl_->rest_worker.joinable()) impl_->rest_worker.join();
  impl_->trace.market_io_qos_status = impl_->market_io_qos_status;
  impl_->trace.rest_io_qos_status = impl_->rest_io_qos_status;
  for (const auto& [asset, balance] : impl_->wallet)
    impl_->trace.balances.push_back({asset, balance.free, balance.locked});
  return std::move(impl_->trace);
}

}  // namespace hquant::bench
