#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "boost/asio/io_context.hpp"

struct sqlite3;

namespace hquant::v1 {

enum class JournalKind : uint8_t {
  // 预留资金并建立待提交订单后、发送下单命令前。
  SubmitIntent,
  // 下单命令被本地发送队列拒绝时，撤销提交意图。
  SubmitAbort,
  // 标记订单待撤后、发送撤单命令前。
  CancelIntent,
  // 撤单命令被本地发送队列拒绝时，撤销撤单意图。
  CancelAbort,
  // 收到下单请求结果，或本地等待响应超时时。
  SubmitOutcome,
  // 收到撤单请求结果，或本地等待响应超时时。
  CancelOutcome,
  // 收到私有订单回报或查单结果时。
  OrderReport,
  // 收到成交回报或补查到成交时。
  Fill,
  // 收到余额绝对值更新或查到账户余额时。
  Balance,
  // 收到余额增减或外部锁定变动事件时。
  BalanceDelta,
  // 收到人工暂停、恢复或全撤指令时。
  Control,
  // 下单请求即将开始网络写入时。
  SubmitWriting,
  // 撤单请求即将开始网络写入时。
  CancelWriting,
};

// 可靠事实；字段含义由 TradingAccount 按 kind 解释。
struct JournalEntry {
  JournalKind kind = JournalKind::SubmitIntent;
  uint64_t sequence = 0;
  std::string client_id;
  std::string symbol;
  std::string asset;
  std::string fee_asset;
  std::string message;
  uint64_t local_id = 0;
  uint64_t shard = 0;
  uint64_t side = 0;
  uint64_t price = 0;
  uint64_t quantity = 0;
  uint64_t remaining = 0;
  uint64_t exchange_id = 0;
  uint64_t trade_id = 0;
  uint64_t gross = 0;
  uint64_t fee = 0;
  uint64_t total = 0;
  uint64_t available = 0;
  uint64_t locked = 0;
  uint64_t amount = 0;
  uint64_t source_bench_seq = 0;
  uint64_t transaction_ms = 0;
  uint64_t result = 0;
  bool credit = false;
  bool external_lock = false;
};

struct JournalControlPage {
  std::vector<JournalEntry> records;
  uint64_t next_cursor = 0;
};

// 可靠日志独立于观察历史：Append 只入队，COMMIT 成功后才投递成功回调。
// 写失败会阻止后续入队；回调所用的 io_context 必须保持存活。
class OrderJournal {
 public:
  using Completion = std::function<void(absl::Status)>;

  explicit OrderJournal(std::string path, size_t capacity = 32768);
  ~OrderJournal();
  OrderJournal(const OrderJournal&) = delete;
  OrderJournal& operator=(const OrderJournal&) = delete;

  absl::Status Start();
  absl::StatusOr<std::vector<JournalEntry>> Load(const std::string& account);
  absl::StatusOr<JournalControlPage> QueryControls(const std::string& account,
                                                   uint32_t limit,
                                                   uint64_t cursor);
  bool Append(const std::string& account, JournalEntry entry,
              boost::asio::io_context& callback_io, Completion done);
  // 排空写队列并等待写线程；不负责执行已投递的回调。
  absl::Status FlushAndStop();

 private:
  // 待提交的事实及其账户线程回调。
  struct Pending {
    std::string account;
    JournalEntry entry;
    boost::asio::io_context* callback_io;
    Completion done;
  };

  void WriterLoop();
  absl::Status WriteBatch(std::vector<Pending>& batch);

  // 文件与有界队列容量。
  std::string path_;
  size_t capacity_;
  // 独立读写连接；运行期间由写线程独占写操作。
  sqlite3* writer_ = nullptr;
  sqlite3* reader_ = nullptr;
  // 写线程从 queue_ 取批次，写入时不持有 mutex_。
  std::thread worker_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<Pending> queue_;
  bool started_ = false;
  bool stopping_ = false;
  // 首次写入错误会保留；已排队记录也收到同一失败结果。
  absl::Status error_;
};

}  // namespace hquant::v1
