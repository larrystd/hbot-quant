// Benchmark-only dyld interposer. Records when kqueue reports a socket as
// readable, keyed by file descriptor. The live benchmark injects this into
// hquant_server only; normal runs never load it.
#include <sys/event.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <cstdint>
#include <vector>

namespace {
constexpr size_t kMaxFd = 1 << 17;
// Each Asio io_context has one worker thread in this benchmark. Keeping the
// stamps on that thread avoids sharing a hot cache line between workers.
thread_local std::vector<uint64_t> read_ready_ns(kMaxFd);

uint64_t NowNs() {
  timespec value{};
  clock_gettime(CLOCK_MONOTONIC, &value);
  return uint64_t(value.tv_sec) * 1'000'000'000ULL + value.tv_nsec;
}
}  // namespace

extern "C" uint64_t hquant_kevent_read_ready_ns(int fd) {
  if (fd < 0 || static_cast<size_t>(fd) >= kMaxFd) return 0;
  return read_ready_ns[fd];
}

extern "C" int hquant_kevent_interpose(int queue,
                                        const struct kevent* changes,
                                        int change_count,
                                        struct kevent* events,
                                        int event_count,
                                        const struct timespec* timeout) {
  // dlsym(RTLD_NEXT, "kevent") resolves back to this interposer on Darwin.
  // This syscall is confined to the optional benchmark library.
  const int count = static_cast<int>(syscall(
      SYS_kevent, queue, changes, change_count, events, event_count, timeout));
  if (count > 0 && events != nullptr) {
    const uint64_t returned_ns = NowNs();
    for (int i = 0; i < count; ++i) {
      if (events[i].filter == EVFILT_READ && events[i].ident < kMaxFd) {
        read_ready_ns[events[i].ident] = returned_ns;
      }
    }
  }
  return count;
}

__attribute__((used)) static struct {
  const void* replacement;
  const void* original;
} interpose_kevent __attribute__((section("__DATA,__interpose"))) = {
    reinterpret_cast<const void*>(&hquant_kevent_interpose),
    reinterpret_cast<const void*>(&kevent)};
