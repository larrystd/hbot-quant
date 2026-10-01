#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "absl/status/statusor.h"
#include "hbot/storage/ports.h"

struct sqlite3;

namespace hbot {

class HistoryReader final : public HistoryReaderPort {
 public:
  struct Options {
    std::string path;
    size_t queue_capacity = 32;
    uint32_t max_page_size = 500;
  };

  static absl::StatusOr<std::unique_ptr<HistoryReader>> Open(Options options);
  ~HistoryReader() override;
  HistoryReader(const HistoryReader&) = delete;
  HistoryReader& operator=(const HistoryReader&) = delete;

  absl::Status TrySubmit(HistoryQuery query) override;
  // nullopt means no response yet. Every accepted query eventually returns a
  // page with matching request_id and OK or non-OK status.
  std::optional<HistoryPage> TryReceive() override;

 private:
  HistoryReader(Options options, sqlite3* db);
  void Run();
  HistoryPage Query(const HistoryQuery& query);

  Options options_;
  sqlite3* db_ = nullptr;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<HistoryQuery> pending_;
  std::deque<HistoryPage> results_;
  size_t outstanding_ = 0;
  bool stopping_ = false;
  std::thread worker_;
};

}  // namespace hbot
