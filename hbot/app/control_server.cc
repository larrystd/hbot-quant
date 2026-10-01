#include "hbot/app/control_server.h"

#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include "hbot/control/wire_codec.h"

namespace hbot {
namespace {

constexpr size_t kMaxFrame = 64 * 1024;
constexpr int kMaxConcurrent = 16;

bool SendAll(int fd, std::string_view bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
#if defined(MSG_NOSIGNAL)
    constexpr int kFlags = MSG_NOSIGNAL;
#else
    constexpr int kFlags = 0;
#endif
    const ssize_t sent =
        send(fd, bytes.data() + offset, bytes.size() - offset, kFlags);
    if (sent <= 0) return false;
    offset += static_cast<size_t>(sent);
  }
  return true;
}

void SendError(int fd, std::string code, std::string message) {
  ControlResponse response;
  response.payload = ControlError{std::move(code), std::move(message)};
  auto frame = EncodeControlResponse(response);
  if (frame.ok()) SendAll(fd, *frame + "\n");
}

}  // namespace

ControlServer::ControlServer(std::string path, Handler handler, int listen_fd)
    : socket_path_(std::move(path)),
      handler_(std::move(handler)),
      listen_fd_(listen_fd) {}

absl::StatusOr<std::unique_ptr<ControlServer>> ControlServer::Start(
    std::string socket_path, Handler handler) {
  if (!handler || socket_path.empty() ||
      socket_path.size() >= sizeof(sockaddr_un::sun_path)) {
    return absl::InvalidArgumentError("invalid control socket path or handler");
  }
  const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) return absl::InternalError(std::strerror(errno));
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size() + 1);
  if (access(socket_path.c_str(), F_OK) == 0) {
    const int probe = socket(AF_UNIX, SOCK_STREAM, 0);
    const bool in_use =
        probe >= 0 && connect(probe, reinterpret_cast<sockaddr*>(&address),
                              sizeof(address)) == 0;
    if (probe >= 0) close(probe);
    if (in_use) {
      close(fd);
      return absl::AlreadyExistsError("control socket is in use");
    }
    if (unlink(socket_path.c_str()) != 0) {
      close(fd);
      return absl::InternalError(std::strerror(errno));
    }
  }
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(fd, kMaxConcurrent) != 0) {
    const std::string error = std::strerror(errno);
    close(fd);
    unlink(socket_path.c_str());
    return absl::InternalError(error);
  }
  auto server = std::unique_ptr<ControlServer>(
      new ControlServer(std::move(socket_path), std::move(handler), fd));
  server->accept_thread_ =
      std::thread([self = server.get()] { self->AcceptLoop(); });
  return server;
}

ControlServer::~ControlServer() { Stop(); }

void ControlServer::Stop() {
  if (stopping_.exchange(true)) return;
  if (listen_fd_ >= 0) {
    shutdown(listen_fd_, SHUT_RDWR);
    close(listen_fd_);
    listen_fd_ = -1;
  }
  if (accept_thread_.joinable()) accept_thread_.join();
  {
    std::lock_guard lock(workers_mutex_);
    for (auto& worker : workers_)
      if (worker.thread.joinable()) worker.thread.join();
  }
  unlink(socket_path_.c_str());
}

void ControlServer::AcceptLoop() {
  while (!stopping_) {
    const int client = accept(listen_fd_, nullptr, nullptr);
    if (client < 0) {
      if (errno == EINTR) continue;
      break;
    }
    if (active_.fetch_add(1) >= kMaxConcurrent) {
      active_.fetch_sub(1);
      SendError(client, "busy", "control server request limit reached");
      close(client);
      continue;
    }
    timeval timeout{2, 0};
    setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#if defined(SO_NOSIGPIPE)
    int no_sigpipe = 1;
    setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe,
               sizeof(no_sigpipe));
#endif
    std::lock_guard lock(workers_mutex_);
    for (auto it = workers_.begin(); it != workers_.end();) {
      if ((*it).done->load()) {
        if ((*it).thread.joinable()) (*it).thread.join();
        it = workers_.erase(it);
      } else
        ++it;
    }
    auto done = std::make_shared<std::atomic<bool>>(false);
    workers_.push_back(Worker{std::thread([this, client, done] {
                                HandleConnection(client);
                                close(client);
                                active_.fetch_sub(1);
                                *done = true;
                              }),
                              std::move(done)});
  }
}

void ControlServer::HandleConnection(int fd) {
  std::string frame;
  frame.reserve(1024);
  char buffer[4096];
  while (frame.size() <= kMaxFrame) {
    const ssize_t read_bytes = recv(fd, buffer, sizeof(buffer), 0);
    if (read_bytes <= 0) return;
    frame.append(buffer, static_cast<size_t>(read_bytes));
    const size_t newline = frame.find('\n');
    if (newline != std::string::npos) {
      frame.resize(newline);
      break;
    }
  }
  if (frame.size() > kMaxFrame) {
    SendError(fd, "frame_too_large", "control frame exceeds 64 KiB");
    return;
  }
  auto request = DecodeControlRequest(frame);
  if (!request.ok()) {
    SendError(fd, "bad_request", std::string(request.status().message()));
    return;
  }
  auto response = handler_(*request);
  response.request_id = request->request_id;
  auto encoded = EncodeControlResponse(response);
  if (!encoded.ok()) {
    SendError(fd, "internal", std::string(encoded.status().message()));
    return;
  }
  SendAll(fd, *encoded + "\n");
}

}  // namespace hbot
