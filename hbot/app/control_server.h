#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "hbot/control/protocol.h"

namespace hbot {

// One JSON request and response line per Unix stream connection. Requests are
// handled on bounded workers so a history query cannot delay status or stop.
class ControlServer {
 public:
  using Handler = std::function<ControlResponse(const ControlRequest&)>;

  static absl::StatusOr<std::unique_ptr<ControlServer>> Start(
      std::string socket_path, Handler handler);
  ~ControlServer();
  ControlServer(const ControlServer&) = delete;
  ControlServer& operator=(const ControlServer&) = delete;

  void Stop();
  const std::string& socket_path() const { return socket_path_; }

 private:
  ControlServer(std::string path, Handler handler, int listen_fd);
  void AcceptLoop();
  void HandleConnection(int fd);

  struct Worker {
    std::thread thread;
    std::shared_ptr<std::atomic<bool>> done;
  };

  std::string socket_path_;
  Handler handler_;
  int listen_fd_ = -1;
  std::atomic<bool> stopping_{false};
  std::atomic<int> active_{0};
  std::thread accept_thread_;
  std::mutex workers_mutex_;
  std::vector<Worker> workers_;
};

}  // namespace hbot
