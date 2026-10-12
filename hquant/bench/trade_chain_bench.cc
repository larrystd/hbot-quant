#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "hquant/bench/venue/sim_venue.h"
#include "hquant/base/log.h"
#include "simdjson.h"
#include "sqlite3.h"

namespace {
namespace fs = std::filesystem;
using hquant::bench::SimVenue;
using hquant::bench::VenueTrace;

struct Options {
  size_t symbols = 1000;
  uint32_t seconds = 30;
  size_t rest_connections = 16;
  fs::path state;
  fs::path server = "bazel-bin/hquant/src/hquant_server";
};

struct Ack {
  uint64_t frame_ns = 0;
  uint64_t read_ns = 0;
  uint64_t parsed_ns = 0;
  uint64_t done_ns = 0;
  uint64_t recv_ns = 0;
};

uint64_t ClockNs() {
  timespec value{};
  clock_gettime(CLOCK_MONOTONIC, &value);
  return uint64_t(value.tv_sec) * 1'000'000'000 + value.tv_nsec;
}

uint64_t Network64(const unsigned char* p) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i) value = (value << 8) | p[i];
  return value;
}

class AckReceiver {
 public:
  AckReceiver() {
    fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd_ < 0) throw std::runtime_error("UDP socket failed");
    int size = 4 * 1024 * 1024;
    setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
      throw std::runtime_error("UDP bind failed");
    socklen_t len = sizeof(addr);
    getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    timeval timeout{0, 200000};
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    worker_ = std::thread([this] { Run(); });
  }

  ~AckReceiver() { Stop(); }
  uint16_t port() const { return port_; }

  void Stop() {
    active_ = false;
    if (worker_.joinable()) worker_.join();
    if (fd_ >= 0) close(fd_);
    fd_ = -1;
  }

  std::map<uint64_t, Ack> Take() { return std::move(acks_); }
  uint64_t malformed() const { return malformed_; }
  uint64_t count() const { return count_; }

 private:
  void Run() {
    while (active_) {
      unsigned char packet[96];
      ssize_t n = recv(fd_, packet, sizeof(packet), 0);
      const uint64_t received = ClockNs();
      if (n < 0) continue;
      if (n != sizeof(packet)) {
        ++malformed_;
        continue;
      }
      const uint64_t id = Network64(packet);
      if (!id || acks_.count(id)) {
        ++malformed_;
        continue;
      }
      acks_.emplace(
          id, Ack{Network64(packet + 8), Network64(packet + 16),
                  Network64(packet + 24), Network64(packet + 32), received});
      ++count_;
    }
  }

  int fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> active_{true};
  std::thread worker_;
  std::map<uint64_t, Ack> acks_;
  uint64_t malformed_ = 0;
  std::atomic<uint64_t> count_{0};
};

void WriteConfig(const fs::path& path, size_t count, size_t rest_connections,
                 hquant::bench::VenuePorts ports) {
  std::ofstream out(path);
  out << "schema_version: 2\ninput:\n  source: binance_public\n"
      << "  rest_host: 127.0.0.1\n  rest_port: " << ports.public_rest
      << "\n  websocket_host: 127.0.0.1\n  websocket_port: " << ports.public_ws
      << "\n  tls: false\naccounts:\n  - id: shared\n"
      << "    mode: pressure_sim\n    venue: binance_spot\n"
      << "    rest_connections: " << rest_connections << "\n"
      << "    trading_rest:\n      host: 127.0.0.1\n      port: "
      << ports.trade_rest
      << "\n      tls: false\n    private_ws:\n      host: 127.0.0.1\n"
      << "      port: " << ports.private_ws
      << "\n      tls: false\n    initial_balances:\n";
  for (size_t i = 0; i < count; ++i)
    out << "      " << SimVenue::Symbol(i) << ": \"1000\"\n";
  out << "      USD: \"1000000000\"\n    hard_limits:\n";
  for (size_t i = 0; i < count; ++i)
    out << "      " << SimVenue::Symbol(i) << ": \"1000\"\n";
  out << "      USD: \"1000000000\"\n"
      << "    maker_fee_rate: \"0.001\"\nshards:\n";
  for (size_t i = 0; i < count; ++i) {
    const std::string symbol = SimVenue::Symbol(i);
    const bool fill = i % 10 == 0;
    out << "  - id: " << i << "\n    market:\n      symbol: " << symbol
        << "\n      base_asset: " << symbol << "\n      quote_asset: USD\n"
        << "      price_per_tick: \"0.01\"\n"
        << "      amount_per_lot: \"1\"\n"
        << "      stale_after_ms: 35000\n"
        << "      max_buffered_diffs: 1024\n"
        << "      trading_rule:\n"
        << "        price_increment: \"0.01\"\n"
        << "        base_increment: \"1\"\n"
        << "        min_base_amount: \"1\"\n"
        << "        min_order_value: \"1\"\n"
        << "    strategy:\n      order_amount: \"" << (fill ? "2" : "1")
        << "\"\n      bid_spread: \"" << (fill ? "0.00005" : "0.001")
        << "\"\n      ask_spread: \"" << (fill ? "0.00005" : "0.001")
        << "\"\n      refresh_ms: " << (fill ? 60000 : 15000)
        << "\n    account_id: shared\n";
  }
  if (!out) throw std::runtime_error("cannot write config");
}

pid_t Launch(const Options& opts, uint16_t ack_port) {
  const pid_t child = fork();
  if (child < 0) throw std::runtime_error("fork failed");
  if (child == 0) {
    int output = open((opts.state / "server.stdout").c_str(),
                      O_CREAT | O_WRONLY | O_TRUNC, 0644);
    int error = open((opts.state / "server.stderr").c_str(),
                     O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (output >= 0) dup2(output, STDOUT_FILENO);
    if (error >= 0) dup2(error, STDERR_FILENO);
    std::string config = "--config=" + (opts.state / "config.yaml").string();
    std::string state = "--state_dir=" + opts.state.string();
    std::string probe =
        "--bench_output=" + (opts.state / "probe.json").string();
    std::string ack = "--bench_ack_port=" + std::to_string(ack_port);
    execl(opts.server.c_str(), opts.server.c_str(), config.c_str(),
          state.c_str(), probe.c_str(), ack.c_str(), nullptr);
    _exit(127);
  }
  return child;
}

bool ChildExited(pid_t child) {
  int status = 0;
  return waitpid(child, &status, WNOHANG) == child;
}

uint64_t PrivateApplies(const fs::path& history) {
  sqlite3* db = nullptr;
  if (sqlite3_open_v2(history.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) !=
      SQLITE_OK)
    throw std::runtime_error("cannot read history.sqlite during drain");
  sqlite3_stmt* statement = nullptr;
  const char* query = "SELECT COUNT(*) FROM account_events WHERE kind=14";
  if (sqlite3_prepare_v2(db, query, -1, &statement, nullptr) != SQLITE_OK) {
    sqlite3_close(db);
    throw std::runtime_error("cannot count private account reports");
  }
  const int result = sqlite3_step(statement);
  const uint64_t count =
      result == SQLITE_ROW
          ? static_cast<uint64_t>(sqlite3_column_int64(statement, 0))
          : 0;
  sqlite3_finalize(statement);
  sqlite3_close(db);
  if (result != SQLITE_ROW)
    throw std::runtime_error("cannot count private account reports");
  return count;
}

void WaitForSettlement(const fs::path& state, const SimVenue& venue,
                       pid_t child) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  auto settled_since = std::chrono::steady_clock::time_point{};
  while (std::chrono::steady_clock::now() < deadline) {
    if (ChildExited(child))
      throw std::runtime_error("hquant_server exited before settlement");
    const uint64_t reported = venue.PrivateReportCount();
    const uint64_t pending = venue.PendingPrivateReports();
    const uint64_t applied = PrivateApplies(state / "history.sqlite");
    const auto now = std::chrono::steady_clock::now();
    if (pending == 0 && applied == reported) {
      if (settled_since == std::chrono::steady_clock::time_point{})
        settled_since = now;
      if (now - settled_since >= std::chrono::seconds(1)) return;
    } else {
      settled_since = {};
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  throw std::runtime_error("private reports did not settle within 30 seconds");
}

bool AccountAction(const fs::path& control, std::string_view op) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return false;
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  const std::string path = control.string();
  if (path.size() >= sizeof(addr.sun_path)) {
    close(fd);
    return false;
  }
  std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);
  if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    close(fd);
    return false;
  }
  const std::string request =
      "{\"request_id\":2,\"op\":\"" + std::string(op) +
      "\",\"account_id\":\"shared\",\"operator\":\"bench\","
      "\"reason\":\"benchmark settlement\"}\n";
  if (write(fd, request.data(), request.size()) !=
      static_cast<ssize_t>(request.size())) {
    close(fd);
    return false;
  }
  char buffer[4096];
  const ssize_t n = read(fd, buffer, sizeof(buffer));
  close(fd);
  return n > 0 && std::string_view(buffer, n).find("\"ok\":true") !=
                      std::string_view::npos;
}

bool StopServer(const fs::path& control) {
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return false;
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  const std::string path = control.string();
  if (path.size() >= sizeof(addr.sun_path)) return false;
  std::memcpy(addr.sun_path, path.c_str(), path.size() + 1);
  if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    close(fd);
    return false;
  }
  const char request[] = "{\"request_id\":1,\"op\":\"stop\"}\n";
  write(fd, request, sizeof(request) - 1);
  char buffer[4096];
  const ssize_t n = read(fd, buffer, sizeof(buffer));
  close(fd);
  return n > 0 && std::string_view(buffer, n).find("\"ok\":true") !=
                      std::string_view::npos;
}

std::string FormatDurationNs(uint64_t ns) {
  double divisor = 1000.0;
  const char* unit = "us";
  if (ns >= 1'000'000'000) {
    divisor = 1'000'000'000.0;
    unit = "s";
  } else if (ns >= 1'000'000) {
    divisor = 1'000'000.0;
    unit = "ms";
  }
  std::ostringstream part;
  part << std::fixed << std::setprecision(3) << ns / divisor << unit;
  return part.str();
}

std::string Distribution(std::vector<uint64_t>& values) {
  if (values.empty()) return "n=0";
  std::sort(values.begin(), values.end());
  auto duration = [&](double p) {
    return FormatDurationNs(
        values[static_cast<size_t>(p * (values.size() - 1))]);
  };
  std::ostringstream out;
  out << "n=" << values.size() << " p50=" << duration(.5)
      << " p95=" << duration(.95) << " p99=" << duration(.99)
      << " max=" << duration(1);
  return out.str();
}

struct LocalEvent {
  int kind = 0;
  std::string client;
  std::string status;
  uint64_t source_seq = 0;
  uint64_t at_ns = 0;
  uint64_t trade_id = 0;
};

struct PrivateClientStamp {
  uint64_t frame_seq = 0;
  uint64_t read_armed_ns = 0;
  uint64_t socket_reads = 0;
  uint64_t frame_complete_ns = 0;
  uint64_t callback_done_ns = 0;
};

using PrivateKey = std::pair<std::string, std::string>;

struct RestClientStamp {
  std::string client_id;
  std::string kind;
  uint64_t enqueued_ns = 0;
  uint64_t dispatched_ns = 0;
  uint64_t write_start_ns = 0;
  uint64_t write_done_ns = 0;
};

struct AccountSubmitStamp {
  uint64_t requested_ns = 0;
  uint64_t started_ns = 0;
};

std::unordered_map<std::string, AccountSubmitStamp> ReadAccountSubmitTrace(
    const fs::path& path) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("missing account_submit_trace.csv");
  std::string line;
  std::getline(input, line);
  std::unordered_map<std::string, AccountSubmitStamp> result;
  while (std::getline(input, line)) {
    std::istringstream stream(line);
    std::string client, requested, started;
    if (!std::getline(stream, client, ',') ||
        !std::getline(stream, requested, ',') ||
        !std::getline(stream, started, ','))
      throw std::runtime_error("invalid account submit trace row");
    if (!result.emplace(std::move(client),
                        AccountSubmitStamp{std::stoull(requested),
                                           std::stoull(started)})
             .second)
      throw std::runtime_error("duplicate account submit trace row");
  }
  return result;
}

std::vector<RestClientStamp> ReadRestClientTrace(const fs::path& path) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("missing rest_client_trace.csv");
  std::string line;
  std::getline(input, line);
  std::vector<RestClientStamp> result;
  while (std::getline(input, line)) {
    std::istringstream stream(line);
    std::string client, kind, queued, dispatched, stamp, write_done;
    if (!std::getline(stream, client, ',') ||
        !std::getline(stream, kind, ',') ||
        !std::getline(stream, queued, ',') ||
        !std::getline(stream, dispatched, ',') ||
        !std::getline(stream, stamp, ',') ||
        !std::getline(stream, write_done, ',') ||
        (kind != "SUBMIT" && kind != "CANCEL"))
      throw std::runtime_error("invalid REST client trace row");
    result.push_back({std::move(client), std::move(kind),
                      std::stoull(queued), std::stoull(dispatched),
                      std::stoull(stamp), std::stoull(write_done)});
  }
  return result;
}

struct PrivateFrameStamp {
  uint64_t frame_seq = 0;
  uint64_t read_started_ns = 0;
  uint64_t read_armed_ns = 0;
  uint64_t frame_complete_ns = 0;
  uint64_t callback_done_ns = 0;
};

struct PrivateClientTrace {
  std::map<PrivateKey, std::deque<PrivateClientStamp>> orders;
  std::vector<PrivateFrameStamp> frames;
};

PrivateClientTrace ReadPrivateClientTrace(const fs::path& path) {
  std::ifstream input(path);
  if (!input) throw std::runtime_error("missing private_client_trace.csv");
  std::string line;
  std::getline(input, line);
  PrivateClientTrace result;
  while (std::getline(input, line)) {
    std::istringstream stream(line);
    std::vector<std::string> fields;
    std::string field;
    while (std::getline(stream, field, ',')) fields.push_back(field);
    if (fields.size() != 15)
      throw std::runtime_error("invalid private client trace row");
    const uint64_t frame_seq = std::stoull(fields[0]);
    const uint64_t callback_done_ns = std::stoull(fields[13]);
    if (fields[1] == "0") {
      result.frames.push_back(
          {frame_seq, std::stoull(fields[5]), std::stoull(fields[6]),
           std::stoull(fields[10]), callback_done_ns});
    } else if (!result.frames.empty() &&
               result.frames.back().frame_seq == frame_seq) {
      result.frames.back().callback_done_ns = callback_done_ns;
    } else {
      throw std::runtime_error("out-of-order private client trace frame");
    }
    if (fields[2] != "ORDER") continue;
    result.orders[{fields[3], fields[4]}].push_back(
        {frame_seq, std::stoull(fields[6]), std::stoull(fields[9]),
         std::stoull(fields[10]), callback_done_ns});
  }
  return result;
}

std::vector<LocalEvent> ReadEvents(const fs::path& db_path) {
  sqlite3* db = nullptr;
  if (sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr) !=
      SQLITE_OK)
    throw std::runtime_error("cannot read history.sqlite");
  sqlite3_stmt* statement = nullptr;
  const char* query =
      "SELECT kind,client_order_id,message,source_bench_seq,"
      "at_mono_ns,trade_id FROM account_events ORDER BY account_sequence";
  if (sqlite3_prepare_v2(db, query, -1, &statement, nullptr) != SQLITE_OK)
    throw std::runtime_error(sqlite3_errmsg(db));
  auto value = [&](int col) -> std::string {
    const unsigned char* raw = sqlite3_column_text(statement, col);
    return raw ? reinterpret_cast<const char*>(raw) : "";
  };
  std::vector<LocalEvent> rows;
  while (sqlite3_step(statement) == SQLITE_ROW) {
    LocalEvent row;
    row.kind = sqlite3_column_int(statement, 0);
    row.client = value(1);
    row.status = value(2);
    const std::string seq = value(3);
    const std::string ns = value(4);
    row.source_seq = seq.empty() ? 0 : std::stoull(seq);
    row.at_ns = ns.empty() ? 0 : std::stoull(ns);
    const std::string trade_id = value(5);
    row.trade_id = trade_id.empty() ? 0 : std::stoull(trade_id);
    rows.push_back(std::move(row));
  }
  sqlite3_finalize(statement);
  sqlite3_close(db);
  return rows;
}

void VerifyBalances(const fs::path& state, const VenueTrace& venue) {
  std::ifstream stream(state / "probe.json");
  if (!stream) throw std::runtime_error("missing probe.json");
  const std::string json((std::istreambuf_iterator<char>(stream)),
                         std::istreambuf_iterator<char>());
  simdjson::dom::parser parser;
  simdjson::dom::element root;
  if (parser.parse(json).get(root))
    throw std::runtime_error("invalid probe.json");
  simdjson::dom::array accounts;
  if (root["final_accounts"].get(accounts) || accounts.size() != 1)
    throw std::runtime_error("invalid final account probe");
  simdjson::dom::element account = accounts.at(0);
  simdjson::dom::element balances;
  if (account["balances"].get(balances))
    throw std::runtime_error("missing final balances");
  uint64_t unknown = 0, open_orders = 0, pending_journal = 0;
  bool manual_paused = false;
  std::string_view error;
  if (account["unknown_orders"].get(unknown) || unknown != 0 ||
      account["open_orders"].get(open_orders) || open_orders != 0 ||
      account["pending_journal"].get(pending_journal) || pending_journal != 0 ||
      account["manual_paused"].get(manual_paused) || !manual_paused ||
      account["error"].get(error) ||
      error != "manually paused: benchmark settlement")
    throw std::runtime_error("final account is not settled");
  for (const auto& stamp : venue.balances) {
    simdjson::dom::element row;
    uint64_t total = 0, free = 0, locked = 0;
    if (balances[stamp.asset].get(row) || row["total_nanos"].get(total) ||
        row["free_nanos"].get(free) || row["locked_nanos"].get(locked) ||
        stamp.free_nanos < 0 || stamp.locked_nanos < 0 ||
        free != static_cast<uint64_t>(stamp.free_nanos) ||
        locked != static_cast<uint64_t>(stamp.locked_nanos) ||
        total != free + locked)
      throw std::runtime_error("venue/local balance mismatch: " + stamp.asset);
  }
  auto* logger = hquant::v1::Logger();
  LOG_INFO(logger, "final balances reconciled={}", venue.balances.size());
  for (const char* stage : {"live_market", "trade_queue_wait"}) {
    simdjson::dom::element row;
    double p50 = 0, p95 = 0, p99 = 0;
    if (!root[stage].get(row) && !row["p50_us"].get(p50) &&
        !row["p95_us"].get(p95) && !row["p99_us"].get(p99)) {
      LOG_INFO(logger, "server probe {}: p50={} p95={} p99={}", stage,
               FormatDurationNs(static_cast<uint64_t>(p50 * 1000)),
               FormatDurationNs(static_cast<uint64_t>(p95 * 1000)),
               FormatDurationNs(static_cast<uint64_t>(p99 * 1000)));
    }
  }
}

void Analyze(const fs::path& state, const VenueTrace& venue,
             const std::map<uint64_t, Ack>& acks, uint64_t malformed,
             uint32_t duration_seconds) {
  std::vector<uint64_t> end_to_end, wire, send_to_frame, frame_to_done;
  std::vector<uint64_t> frame_to_read, read_to_parse, parse_to_done;
  std::vector<uint64_t> done_to_ack, read_to_done, market_to_order;
  std::vector<uint64_t> report_to_apply;
  std::vector<uint64_t> market_to_report, quote_to_fill, action_to_fill;
  std::ofstream market(state / "market_trace.csv");
  std::ofstream private_venue(state / "private_venue_trace.csv");
  private_venue << "client_id,status,write_start_ns,write_done_ns,"
                   "exchange_order_id,trade_id\n";
  for (const auto& report : venue.reports)
    private_venue << report.client_id << ',' << report.status << ','
                  << report.at_ns << ',' << report.write_done_ns << ','
                  << report.exchange_order_id << ',' << report.trade_id
                  << '\n';
  std::ofstream rest_venue(state / "rest_venue_trace.csv");
  rest_venue << "client_id,kind,request_complete_ns\n";
  for (const auto& request : venue.requests)
    rest_venue << request.client_id << ',' << request.status << ','
               << request.at_ns << '\n';
  market << "bench_seq,send_ns,frame_ns,read_ns,parsed_ns,done_ns,recv_ns\n";
  for (size_t seq = 1; seq < venue.market_send_ns.size(); ++seq) {
    auto it = acks.find(seq);
    if (it == acks.end()) continue;
    const uint64_t sent = venue.market_send_ns[seq];
    const Ack& ack = it->second;
    market << seq << ',' << sent << ',' << ack.frame_ns << ',' << ack.read_ns
           << ',' << ack.parsed_ns << ',' << ack.done_ns << ',' << ack.recv_ns
           << '\n';
    if (sent && ack.recv_ns >= sent) end_to_end.push_back(ack.recv_ns - sent);
    if (sent && ack.done_ns >= sent) wire.push_back(ack.done_ns - sent);
    if (sent && ack.frame_ns >= sent)
      send_to_frame.push_back(ack.frame_ns - sent);
    if (ack.done_ns >= ack.frame_ns)
      frame_to_done.push_back(ack.done_ns - ack.frame_ns);
    if (ack.read_ns >= ack.frame_ns)
      frame_to_read.push_back(ack.read_ns - ack.frame_ns);
    if (ack.parsed_ns >= ack.read_ns)
      read_to_parse.push_back(ack.parsed_ns - ack.read_ns);
    if (ack.done_ns >= ack.parsed_ns)
      parse_to_done.push_back(ack.done_ns - ack.parsed_ns);
    if (ack.done_ns >= ack.read_ns)
      read_to_done.push_back(ack.done_ns - ack.read_ns);
    if (ack.recv_ns >= ack.done_ns)
      done_to_ack.push_back(ack.recv_ns - ack.done_ns);
  }
  std::map<PrivateKey, std::deque<VenueTrace::AccountStamp>> reports;
  std::map<std::pair<std::string, std::string>, std::deque<uint64_t>> requests;
  auto private_client =
      ReadPrivateClientTrace(state / "private_client_trace.csv");
  for (const auto& event : venue.reports)
    reports[{event.client_id, event.status}].push_back(event);
  for (const auto& event : venue.requests)
    requests[{event.client_id, event.status}].push_back(event.at_ns);
  auto wire_requests = requests;
  const auto account_submits =
      ReadAccountSubmitTrace(state / "account_submit_trace.csv");
  std::unordered_map<std::string, RestClientStamp> rest_submits;
  std::vector<uint64_t> submit_request_total, cancel_request_total;
  std::vector<uint64_t> submit_request_wire, cancel_request_wire;
  for (const auto& sent : ReadRestClientTrace(state / "rest_client_trace.csv")) {
    auto found = wire_requests.find({sent.client_id, sent.kind});
    if (found == wire_requests.end() || found->second.empty() ||
        sent.dispatched_ns < sent.enqueued_ns ||
        sent.write_start_ns < sent.dispatched_ns ||
        sent.write_done_ns < sent.write_start_ns ||
        found->second.front() < sent.write_start_ns)
      throw std::runtime_error("unmatched REST request trace");
    const uint64_t total = found->second.front() - sent.enqueued_ns;
    const uint64_t wire = found->second.front() - sent.write_start_ns;
    found->second.pop_front();
    (sent.kind == "SUBMIT" ? submit_request_total : cancel_request_total)
        .push_back(total);
    (sent.kind == "SUBMIT" ? submit_request_wire : cancel_request_wire)
        .push_back(wire);
    if (sent.kind == "SUBMIT" &&
        !rest_submits.emplace(sent.client_id, sent).second)
      throw std::runtime_error("duplicate REST submit trace");
  }
  if (submit_request_wire.size() != venue.account_submits ||
      cancel_request_wire.size() != venue.account_cancels ||
      account_submits.size() != venue.account_submits)
    throw std::runtime_error("REST request trace count mismatch");
  std::vector<uint64_t> all_account_queue, all_account_process;
  for (const auto& [client, rest] : rest_submits) {
    auto account = account_submits.find(client);
    if (account == account_submits.end() ||
        account->second.requested_ns > account->second.started_ns ||
        account->second.started_ns > rest.enqueued_ns)
      throw std::runtime_error("unmatched account submit trace");
    all_account_queue.push_back(account->second.started_ns -
                                account->second.requested_ns);
    all_account_process.push_back(rest.enqueued_ns -
                                  account->second.started_ns);
  }
  std::unordered_map<std::string, uint64_t> submit_receive_ns;
  for (const auto& event : venue.requests)
    if (event.status == "SUBMIT")
      submit_receive_ns.emplace(event.client_id, event.at_ns);
  std::unordered_map<uint64_t, VenueTrace::MatchStamp> action_by_trade;
  for (const auto& match : venue.matches)
    action_by_trade.emplace(match.trade_id, match);
  std::unordered_map<std::string, uint64_t> source;
  std::ofstream account(state / "account_trace.csv");
  account << "kind,client_id,status,source_bench_seq,market_send_ns,event_ns,"
             "venue_ns,trade_id,match_market_seq,match_action_ns\n";
  const std::map<std::string, std::string> names = {{"1", "REJECTED"},
                                                    {"2", "NEW"},
                                                    {"3", "PARTIALLY_FILLED"},
                                                    {"4", "FILLED"},
                                                    {"5", "CANCELED"}};
  size_t submits = 0, cancels = 0, fills = 0, applied = 0;
  size_t timeouts = 0, queue_expired = 0;
  size_t first_new = 0, read_after_write = 0, buffered_new = 0;
  size_t frame_before_write_done = 0;
  const size_t first_wave_new = 2 * (venue.balances.size() - 1);
  uint64_t first_write_start = std::numeric_limits<uint64_t>::max();
  uint64_t last_write_start = 0;
  uint64_t first_frame_complete = std::numeric_limits<uint64_t>::max();
  uint64_t last_frame_complete = 0;
  uint64_t first_frame_seq = std::numeric_limits<uint64_t>::max();
  uint64_t last_frame_seq = 0;
  std::vector<uint64_t> write_done_to_read_arm, read_arm_to_frame;
  std::vector<uint64_t> frame_to_callback_done, callback_done_to_apply;
  std::vector<uint64_t> market_to_private_frame;
  std::vector<uint64_t> order_market_to_frame, order_frame_to_parse;
  std::vector<uint64_t> order_parse_to_account_request;
  std::vector<uint64_t> order_account_queue, order_account_process;
  std::vector<uint64_t> order_rest_queue, order_rest_prepare, order_rest_wire;
  std::vector<uint64_t> rest_receive_to_private_write;
  std::vector<uint64_t> venue_write_start_to_private_frame;
  std::vector<uint64_t> private_frame_to_apply;
  for (const auto& row : ReadEvents(state / "history.sqlite")) {
    if (row.kind == 1) {
      ++submits;
      source[row.client] = row.source_seq;
      if (row.source_seq != 0) {
        auto market = acks.find(row.source_seq);
        auto account = account_submits.find(row.client);
        auto rest = rest_submits.find(row.client);
        auto received = submit_receive_ns.find(row.client);
        if (market == acks.end() || account == account_submits.end() ||
            rest == rest_submits.end() || received == submit_receive_ns.end())
          throw std::runtime_error("unmatched market-triggered submit trace");
        const uint64_t send_ns = venue.market_send_ns.at(row.source_seq);
        const Ack& ack = market->second;
        const auto& task = account->second;
        const auto& request = rest->second;
        if (!(send_ns <= ack.frame_ns &&
              ack.frame_ns <= ack.parsed_ns &&
              ack.parsed_ns <= task.requested_ns &&
              task.requested_ns <= task.started_ns &&
              task.started_ns <= request.enqueued_ns &&
              request.enqueued_ns <= request.dispatched_ns &&
              request.dispatched_ns <= request.write_start_ns &&
              request.write_start_ns <= received->second))
          throw std::runtime_error("out-of-order market submit trace");
        order_market_to_frame.push_back(ack.frame_ns - send_ns);
        order_frame_to_parse.push_back(ack.parsed_ns - ack.frame_ns);
        order_parse_to_account_request.push_back(task.requested_ns -
                                                 ack.parsed_ns);
        order_account_queue.push_back(task.started_ns - task.requested_ns);
        order_account_process.push_back(request.enqueued_ns - task.started_ns);
        order_rest_queue.push_back(request.dispatched_ns -
                                   request.enqueued_ns);
        order_rest_prepare.push_back(request.write_start_ns -
                                     request.dispatched_ns);
        order_rest_wire.push_back(received->second - request.write_start_ns);
      }
    }
    if (row.kind == 2) ++cancels;
    if (row.kind == 6) ++fills;
    if (row.kind == 14) ++applied;
    if (row.kind == 11) ++timeouts;
    if (row.kind == 13) ++queue_expired;
    const uint64_t seq = row.source_seq ? row.source_seq : source[row.client];
    const uint64_t sent =
        seq < venue.market_send_ns.size() ? venue.market_send_ns[seq] : 0;
    uint64_t venue_ns = 0;
    uint64_t match_seq = 0, match_ns = 0;
    if (row.kind == 1 || row.kind == 2) {
      auto& q = requests[{row.client, row.kind == 1 ? "SUBMIT" : "CANCEL"}];
      if (!q.empty()) {
        venue_ns = q.front();
        q.pop_front();
        if (row.kind == 1 && sent && venue_ns >= sent)
          market_to_order.push_back(venue_ns - sent);
      }
    }
    if (row.kind == 14) {
      auto name = names.find(row.status);
      if (name != names.end()) {
        const PrivateKey key{row.client, name->second};
        auto& venue_q = reports[key];
        auto& client_q = private_client.orders[key];
        if (venue_q.empty() || client_q.empty())
          throw std::runtime_error("unmatched private order trace");
        const auto venue_stamp = std::move(venue_q.front());
        venue_q.pop_front();
        const auto client_stamp = client_q.front();
        client_q.pop_front();
        venue_ns = venue_stamp.at_ns;
        if (row.at_ns >= venue_ns)
          report_to_apply.push_back(row.at_ns - venue_ns);
        if (name->second == "NEW" && first_new++ < first_wave_new) {
          first_write_start =
              std::min(first_write_start, venue_stamp.at_ns);
          last_write_start =
              std::max(last_write_start, venue_stamp.at_ns);
          first_frame_complete =
              std::min(first_frame_complete, client_stamp.frame_complete_ns);
          last_frame_complete =
              std::max(last_frame_complete, client_stamp.frame_complete_ns);
          first_frame_seq = std::min(first_frame_seq, client_stamp.frame_seq);
          last_frame_seq = std::max(last_frame_seq, client_stamp.frame_seq);
          if (sent && client_stamp.frame_complete_ns >= sent)
            market_to_private_frame.push_back(
                client_stamp.frame_complete_ns - sent);
          auto received = submit_receive_ns.find(row.client);
          if (received != submit_receive_ns.end() &&
              venue_stamp.at_ns >= received->second)
            rest_receive_to_private_write.push_back(
                venue_stamp.at_ns - received->second);
          if (client_stamp.frame_complete_ns >= venue_stamp.at_ns)
            venue_write_start_to_private_frame.push_back(
                client_stamp.frame_complete_ns - venue_stamp.at_ns);
          if (client_stamp.frame_complete_ns < venue_stamp.write_done_ns)
            ++frame_before_write_done;
          if (row.at_ns >= client_stamp.frame_complete_ns)
            private_frame_to_apply.push_back(
                row.at_ns - client_stamp.frame_complete_ns);
          if (client_stamp.read_armed_ns >= venue_stamp.write_done_ns) {
            ++read_after_write;
            write_done_to_read_arm.push_back(
                client_stamp.read_armed_ns - venue_stamp.write_done_ns);
            if (client_stamp.frame_complete_ns >=
                client_stamp.read_armed_ns)
              read_arm_to_frame.push_back(client_stamp.frame_complete_ns -
                                          client_stamp.read_armed_ns);
          }
          if (client_stamp.socket_reads == 0) ++buffered_new;
          if (client_stamp.callback_done_ns >=
              client_stamp.frame_complete_ns)
            frame_to_callback_done.push_back(
                client_stamp.callback_done_ns -
                client_stamp.frame_complete_ns);
          if (row.at_ns >= client_stamp.callback_done_ns)
            callback_done_to_apply.push_back(
                row.at_ns - client_stamp.callback_done_ns);
        }
      }
      if ((row.status == "1" || row.status == "2") && sent && row.at_ns >= sent)
        market_to_report.push_back(row.at_ns - sent);
    }
    if (row.kind == 6) {
      if (sent && row.at_ns >= sent) quote_to_fill.push_back(row.at_ns - sent);
      auto match = action_by_trade.find(row.trade_id);
      if (match != action_by_trade.end()) {
        match_seq = match->second.market_seq;
        match_ns = match->second.action_ns;
        if (row.at_ns >= match_ns)
          action_to_fill.push_back(row.at_ns - match_ns);
      }
    }
    if (row.kind == 1 || row.kind == 2 || row.kind == 6 || row.kind == 14)
      account << row.kind << ',' << row.client << ',' << row.status << ','
              << seq << ',' << sent << ',' << row.at_ns << ',' << venue_ns
              << ',' << row.trade_id << ',' << match_seq << ',' << match_ns
              << '\n';
  }
  auto* logger = hquant::v1::Logger();
  LOG_INFO(logger, "market messages={} ACK={} actions={} rate={}/s malformed={}",
           venue.market_messages, acks.size(), venue.market_actions,
           venue.market_messages / duration_seconds, malformed);
  LOG_INFO(logger,
           "account submit={} cancel={} fill={} private_apply={} "
           "venue_submit={} venue_cancel={} venue_fill={} "
           "venue_private_reports={} local_timeouts={} local_queue_expired={}",
           submits, cancels, fills, applied, venue.account_submits,
           venue.account_cancels, venue.account_fills, venue.private_reports,
           timeouts, queue_expired);
  LOG_INFO(logger,
           "PRIMARY quant POST enqueued -> venue HTTP request complete: {}",
           Distribution(submit_request_total));
  LOG_INFO(logger,
           "PRIMARY quant DELETE enqueued -> venue HTTP request complete: {}",
           Distribution(cancel_request_total));
  LOG_INFO(logger, "all submits account task queue wait: {}",
           Distribution(all_account_queue));
  LOG_INFO(logger, "all submits account process -> REST enqueue: {}",
           Distribution(all_account_process));
  LOG_INFO(logger, "market-triggered submit market send -> frame: {}",
           Distribution(order_market_to_frame));
  LOG_INFO(logger, "market-triggered submit frame -> parsed: {}",
           Distribution(order_frame_to_parse));
  LOG_INFO(logger, "market-triggered submit parsed -> account request: {}",
           Distribution(order_parse_to_account_request));
  LOG_INFO(logger, "market-triggered submit account task queue wait: {}",
           Distribution(order_account_queue));
  LOG_INFO(logger, "market-triggered submit account process -> REST enqueue: {}",
           Distribution(order_account_process));
  LOG_INFO(logger, "market-triggered submit REST enqueue -> dispatch: {}",
           Distribution(order_rest_queue));
  LOG_INFO(logger, "market-triggered submit REST dispatch -> write start: {}",
           Distribution(order_rest_prepare));
  LOG_INFO(logger, "market-triggered submit REST write start -> venue receive: {}",
           Distribution(order_rest_wire));
  LOG_INFO(logger,
           "quant REST POST write start -> venue HTTP request complete: {}",
           Distribution(submit_request_wire));
  LOG_INFO(logger,
           "quant REST DELETE write start -> venue HTTP request complete: {}",
           Distribution(cancel_request_wire));
  LOG_INFO(logger, "market send -> ACK received (T0->T9): {}",
           Distribution(end_to_end));
  LOG_INFO(logger, "market send -> server done (T0->T6): {}",
           Distribution(wire));
  LOG_INFO(logger, "market send -> WS frame complete: {}",
           Distribution(send_to_frame));
  LOG_INFO(logger, "WS frame complete -> server done (T3->T6): {}",
           Distribution(frame_to_done));
  LOG_INFO(logger, "WS frame complete -> feed read: {}",
           Distribution(frame_to_read));
  LOG_INFO(logger, "server read -> done: {}", Distribution(read_to_done));
  LOG_INFO(logger, "server read -> parsed: {}", Distribution(read_to_parse));
  LOG_INFO(logger, "server parsed -> market callback done: {}",
           Distribution(parse_to_done));
  LOG_INFO(logger, "server done -> ACK received: {}",
           Distribution(done_to_ack));
  LOG_INFO(logger, "market send -> venue submit: {}",
           Distribution(market_to_order));
  LOG_INFO(logger, "private report send -> local apply: {}",
           Distribution(report_to_apply));
  LOG_INFO(logger, "market send -> client private WS frame complete: {}",
           Distribution(market_to_private_frame));
  LOG_INFO(logger, "venue REST receive -> private WS write start: {}",
           Distribution(rest_receive_to_private_write));
  LOG_INFO(logger, "private WS write start -> client frame complete: {}",
           Distribution(venue_write_start_to_private_frame));
  LOG_INFO(logger, "client private WS frame complete -> local apply: {}",
           Distribution(private_frame_to_apply));
  LOG_INFO(logger,
           "first NEW private reports={} read_armed_after_write_callback={} "
           "frame_before_write_callback={} no_socket_read={}",
           std::min(first_new, first_wave_new), read_after_write,
           frame_before_write_done, buffered_new);
  LOG_INFO(logger, "venue write done -> client read armed: {}",
           Distribution(write_done_to_read_arm));
  LOG_INFO(logger, "client read armed -> WS frame complete: {}",
           Distribution(read_arm_to_frame));
  LOG_INFO(logger, "WS frame complete -> account callback done: {}",
           Distribution(frame_to_callback_done));
  LOG_INFO(logger, "account callback done -> local apply: {}",
           Distribution(callback_done_to_apply));
  size_t burst_frames = 0;
  uint64_t burst_read_await_ns = 0, burst_callback_ns = 0;
  uint64_t burst_between_reads_ns = 0;
  const PrivateFrameStamp* previous = nullptr;
  for (const auto& frame : private_client.frames) {
    if (frame.frame_seq < first_frame_seq || frame.frame_seq > last_frame_seq)
      continue;
    ++burst_frames;
    if (previous && frame.frame_seq == previous->frame_seq + 1 &&
        frame.frame_complete_ns >= frame.read_armed_ns &&
        previous->callback_done_ns >= previous->frame_complete_ns &&
        frame.read_armed_ns >= previous->callback_done_ns) {
      burst_read_await_ns += frame.frame_complete_ns - frame.read_armed_ns;
      burst_callback_ns +=
          previous->callback_done_ns - previous->frame_complete_ns;
      burst_between_reads_ns +=
          frame.read_armed_ns - previous->callback_done_ns;
    }
    previous = &frame;
  }
  if (first_write_start <= last_write_start &&
      first_frame_complete <= last_frame_complete) {
    LOG_INFO(logger,
             "first private burst: frames={} venue_write_span={} "
             "client_frame_span={} read_await_sum={} callback_sum={} "
             "between_reads_sum={}",
             burst_frames,
             FormatDurationNs(last_write_start - first_write_start),
             FormatDurationNs(last_frame_complete - first_frame_complete),
             FormatDurationNs(burst_read_await_ns),
             FormatDurationNs(burst_callback_ns),
             FormatDurationNs(burst_between_reads_ns));
  }
  LOG_INFO(logger, "market send -> first private order apply: {}",
           Distribution(market_to_report));
  LOG_INFO(logger, "external match action -> fill apply: {}",
           Distribution(action_to_fill));
  LOG_INFO(logger, "original quote market -> eventual fill apply: {}",
           Distribution(quote_to_fill));
  LOG_INFO(logger, "trace: {} {} {} {} {} {} {} {}",
           (state / "market_trace.csv").string(),
           (state / "account_trace.csv").string(),
           (state / "account_submit_trace.csv").string(),
           (state / "shared_view_flush_trace.csv").string(),
           (state / "rest_client_trace.csv").string(),
           (state / "rest_venue_trace.csv").string(),
           (state / "private_venue_trace.csv").string(),
           (state / "private_client_trace.csv").string());
  if (acks.size() != venue.market_messages || malformed ||
      fills != venue.account_fills || applied != venue.private_reports)
    throw std::runtime_error("market/account trace count mismatch");
  VerifyBalances(state, venue);
}

Options Parse(int argc, char** argv) {
  Options opts;
  for (int i = 1; i < argc; ++i) {
    std::string key(argv[i]);
    if (i + 1 >= argc)
      throw std::runtime_error("option requires value: " + key);
    const std::string value(argv[++i]);
    if (key == "--symbols")
      opts.symbols = std::stoull(value);
    else if (key == "--seconds")
      opts.seconds = std::stoul(value);
    else if (key == "--rest-connections")
      opts.rest_connections = std::stoull(value);
    else if (key == "--state-dir")
      opts.state = value;
    else if (key == "--server")
      opts.server = value;
    else
      throw std::runtime_error("unknown option: " + key);
  }
  if (opts.symbols < 1 || opts.symbols > 1000 || opts.seconds < 1 ||
      opts.seconds > 180 || opts.rest_connections < 1 ||
      opts.rest_connections > 32)
    throw std::runtime_error(
        "symbols must be 1..1000, seconds 1..180, rest connections 1..32");
  if (opts.state.empty()) {
    opts.state = fs::temp_directory_path() /
                 ("hquant-cpp-trade-" + std::to_string(getpid()));
  }
  opts.state = fs::absolute(opts.state);
  opts.server = fs::absolute(opts.server);
  return opts;
}
}  // namespace

int main(int argc, char** argv) {
  pid_t child = -1;
  try {
    const Options opts = Parse(argc, argv);
    if (fs::exists(opts.state / "history.sqlite"))
      throw std::runtime_error("state directory already has history.sqlite");
    fs::create_directories(opts.state);
    rlimit limit{};
    if (getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur < 8192) {
      limit.rlim_cur = std::min<rlim_t>(8192, limit.rlim_max);
      setrlimit(RLIMIT_NOFILE, &limit);
    }
    SimVenue venue(opts.symbols);
    venue.Start();
    WriteConfig(opts.state / "config.yaml", opts.symbols, opts.rest_connections,
                venue.ports());
    AckReceiver receiver;
    child = Launch(opts, receiver.port());
    const auto startup = std::chrono::steady_clock::now();
    while (!venue.Ready()) {
      if (ChildExited(child))
        throw std::runtime_error("hquant_server exited on startup");
      if (std::chrono::steady_clock::now() - startup >
          std::chrono::seconds(120))
        throw std::runtime_error("startup timed out");
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOG_INFO(hquant::v1::Logger(), "ready: symbols={} startup_s={:.3f} state={}",
             opts.symbols,
             std::chrono::duration<double>(
                 std::chrono::steady_clock::now() - startup)
                 .count(),
             opts.state.string());
    venue.BeginReplay(opts.seconds);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(opts.seconds + 60);
    while (!venue.ReplayDone()) {
      if (ChildExited(child))
        throw std::runtime_error("hquant_server exited during replay");
      if (std::chrono::steady_clock::now() > deadline)
        throw std::runtime_error("replay timed out");
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    const auto ack_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (receiver.count() < venue.MarketMessageCount()) {
      if (ChildExited(child))
        throw std::runtime_error("hquant_server exited before all ACKs");
      if (std::chrono::steady_clock::now() > ack_deadline)
        throw std::runtime_error("market completion ACKs timed out");
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (!AccountAction(opts.state / "control.sock", "pause_new_orders") ||
        !AccountAction(opts.state / "control.sock", "cancel_all"))
      throw std::runtime_error("account settlement control failed");
    WaitForSettlement(opts.state, venue, child);
    if (!StopServer(opts.state / "control.sock"))
      throw std::runtime_error("control stop failed");
    int exit_status = 0;
    if (waitpid(child, &exit_status, 0) != child || !WIFEXITED(exit_status) ||
        WEXITSTATUS(exit_status) != 0)
      throw std::runtime_error("hquant_server failed during stop");
    child = -1;
    const VenueTrace trace = venue.StopAndCollect();
    receiver.Stop();
    const uint64_t malformed = receiver.malformed();
    Analyze(opts.state, trace, receiver.Take(), malformed, opts.seconds);
    LOG_INFO(hquant::v1::Logger(), "valid=true");
    return 0;
  } catch (const std::exception& error) {
    if (child > 0) {
      kill(child, SIGTERM);
      waitpid(child, nullptr, 0);
    }
    LOG_ERROR(hquant::v1::Logger(), "benchmark failed: {}", error.what());
    return 1;
  }
}
