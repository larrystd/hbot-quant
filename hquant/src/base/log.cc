#include "hquant/base/log.h"

#include "quill/Backend.h"
#include "quill/Frontend.h"
#include "quill/Logger.h"
#include "quill/sinks/ConsoleSink.h"

namespace hquant::v1 {
namespace {

class LoggingState {
 public:
  LoggingState() {
    quill::Backend::start();
    quill::ConsoleSinkConfig config;
    config.set_stream("stderr");
    auto sink = quill::Frontend::create_or_get_sink<quill::ConsoleSink>(
        "hquant_stderr", config);
    logger_ = quill::Frontend::create_or_get_logger("hquant", sink);
  }

  ~LoggingState() { quill::Backend::stop(); }

  quill::Logger* Get() const { return logger_; }

 private:
  quill::Logger* logger_ = nullptr;
};

}  // namespace

quill::Logger* Logger() {
  static LoggingState logging;
  return logging.Get();
}

}  // namespace hquant::v1
