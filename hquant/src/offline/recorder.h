#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "offline/storage.h"

struct sqlite3;

namespace hquant {

class SqliteRecorder final : public RecorderPort {
 public:
  struct Options {
    std::string path;
    RunId run_id;
    UtcTime started_at_utc{};
    size_t queue_capacity_per_shard = 1024;
    size_t batch_size = 64;
  };

  static absl::StatusOr<std::unique_ptr<SqliteRecorder>> Open(Options options);
  ~SqliteRecorder() override;
  SqliteRecorder(const SqliteRecorder&) = delete;
  SqliteRecorder& operator=(const SqliteRecorder&) = delete;

  // One producer per shard. Never waits for a SQLite commit or acquires a
  // mutex. The caller allocates shard_sequence before calling, even if this
  // returns false.
  bool TryPush(RecordEnvelope record) override;

  // Control/test thread operations. Call Stop only after producers have
  // stopped.
  absl::Status Flush();
  absl::Status Stop(UtcTime clean_stopped_at_utc);
  StorageHealth Health() const;
  RunManifest Manifest() const;

  // Deterministic fault injection for the package tests; no SQLite operation
  // occurs on a shard thread.
  void SetWriteFailureForTesting(bool enabled);
  void PauseWorkerForTesting(bool paused);

 private:
  struct Queue {
    explicit Queue(size_t capacity) : slots(capacity) {}
    std::vector<std::optional<RecordEnvelope>> slots;
    alignas(64) std::atomic<size_t> head{0};
    alignas(64) std::atomic<size_t> tail{0};
    std::atomic<uint64_t> last_attempted_seq{0};
    std::atomic<uint64_t> last_dropped_seq{0};
    std::atomic<uint64_t> last_accounted_dropped_seq{0};
    std::atomic<uint64_t> dropped_count{0};
  };

  SqliteRecorder(Options options, sqlite3* db);
  void Run();
  bool Pop(uint8_t shard, RecordEnvelope* record);
  void AddGap(const HistoryGap& gap);
  absl::Status WriteBatch(const std::vector<RecordEnvelope>& batch);
  absl::Status PersistPendingGaps();
  absl::Status FinishManifest(UtcTime clean_time);
  void SetError(const absl::Status& status);

  Options options_;
  sqlite3* db_ = nullptr;
  std::array<std::unique_ptr<Queue>, 8> queues_;
  std::array<uint64_t, 8> last_observed_seq_{};
  std::atomic<size_t> pending_records_{0};
  std::atomic<size_t> queue_watermark_{0};
  std::atomic<bool> fail_writes_for_testing_{false};
  std::atomic<bool> pause_worker_for_testing_{false};
  bool worker_paused_ = false;
  std::mutex wake_mutex_;
  std::condition_variable wake_cv_;
  std::thread worker_;
  bool stopping_ = false;
  std::optional<UtcTime> stop_time_;
  bool processing_ = false;
  absl::Status final_status_;

  mutable std::mutex state_mutex_;
  StorageHealth health_;
  RunManifest manifest_;
  std::vector<HistoryGap> pending_gaps_;  // worker thread only
};

}  // namespace hquant
