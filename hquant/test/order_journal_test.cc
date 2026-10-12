#include <filesystem>
#include <future>
#include <iostream>
#include <string>
#include <thread>

#include "boost/asio/executor_work_guard.hpp"
#include "boost/asio/io_context.hpp"
#include "hquant/trading/order_journal.h"
#include "sqlite3.h"

int main() {
  const std::string path =
      (std::filesystem::temp_directory_path() /
       "hquant-order-journal-test.sqlite")
          .string();
  std::filesystem::remove(path);
  std::filesystem::remove(path + "-wal");
  std::filesystem::remove(path + "-shm");
  boost::asio::io_context io;
  auto guard = boost::asio::make_work_guard(io);
  std::thread worker([&io] { io.run(); });
  bool good = true;
  {
    hquant::v1::OrderJournal journal(path);
    if (!journal.Start().ok()) good = false;
    hquant::v1::JournalEntry entry;
    entry.kind = hquant::v1::JournalKind::SubmitIntent;
    entry.sequence = 1;
    entry.client_id = "client-1";
    entry.symbol = "BTCUSDT";
    entry.quantity = 123;
    std::promise<absl::Status> committed;
    if (!journal.Append("account", entry, io,
                        [&committed](absl::Status status) {
                          committed.set_value(status);
                        }) ||
        !committed.get_future().get().ok()) {
      good = false;
    }
    if (!journal.FlushAndStop().ok()) good = false;
  }
  {
    hquant::v1::OrderJournal journal(path);
    if (!journal.Start().ok()) good = false;
    auto loaded = journal.Load("account");
    if (!loaded.ok() || loaded->size() != 1 ||
        (*loaded)[0].client_id != "client-1" ||
        (*loaded)[0].symbol != "BTCUSDT" ||
        (*loaded)[0].quantity != 123) {
      good = false;
    }
    hquant::v1::JournalEntry control;
    control.kind = hquant::v1::JournalKind::Control;
    control.sequence = 2;
    control.result = 0;
    control.asset = "operator";
    control.message = "test pause";
    std::promise<absl::Status> committed;
    if (!journal.Append("account", control, io,
                        [&committed](absl::Status status) {
                          committed.set_value(status);
                        }) ||
        !committed.get_future().get().ok()) {
      good = false;
    }
    auto audit = journal.QueryControls("account", 1, 0);
    if (!audit.ok() || audit->records.size() != 1 ||
        audit->records[0].sequence != 2 ||
        audit->records[0].asset != "operator" ||
        audit->records[0].message != "test pause" ||
        audit->next_cursor != 2) {
      good = false;
    }
    auto empty = journal.QueryControls("account", 1, 2);
    if (!empty.ok() || !empty->records.empty() || empty->next_cursor != 2) {
      good = false;
    }
    if (!journal.FlushAndStop().ok()) good = false;
  }
  {
    sqlite3* db = nullptr;
    if (sqlite3_open(path.c_str(), &db) != SQLITE_OK ||
        sqlite3_exec(db,
                     "CREATE TRIGGER fail_journal BEFORE INSERT ON events "
                     "BEGIN SELECT RAISE(FAIL, 'injected write failure'); END",
                     nullptr, nullptr, nullptr) != SQLITE_OK) {
      good = false;
    }
    if (db) sqlite3_close(db);
    hquant::v1::OrderJournal journal(path);
    if (!journal.Start().ok()) good = false;
    hquant::v1::JournalEntry entry;
    entry.kind = hquant::v1::JournalKind::SubmitIntent;
    entry.sequence = 3;
    entry.client_id = "unsent-client";
    std::promise<absl::Status> failed;
    if (!journal.Append("account", entry, io,
                        [&failed](absl::Status status) {
                          failed.set_value(status);
                        })) {
      good = false;
    } else if (failed.get_future().get().ok()) {
      good = false;
    }
    if (journal.Append("account", entry, io, [](absl::Status) {})) {
      good = false;
    }
    if (journal.FlushAndStop().ok()) good = false;
    if (sqlite3_open(path.c_str(), &db) != SQLITE_OK ||
        sqlite3_exec(db, "DROP TRIGGER fail_journal", nullptr, nullptr,
                     nullptr) != SQLITE_OK) {
      good = false;
    }
    if (db) sqlite3_close(db);
    hquant::v1::OrderJournal recovered(path);
    if (!recovered.Start().ok()) good = false;
    auto loaded = recovered.Load("account");
    if (!loaded.ok() || loaded->size() != 2 ||
        loaded->back().kind != hquant::v1::JournalKind::Control) {
      good = false;
    }
    if (!recovered.FlushAndStop().ok()) good = false;
  }
  guard.reset();
  worker.join();
  std::filesystem::remove(path);
  std::filesystem::remove(path + "-wal");
  std::filesystem::remove(path + "-shm");
  if (!good) {
    std::cerr << "order journal durability test failed\n";
    return 1;
  }
  return 0;
}
