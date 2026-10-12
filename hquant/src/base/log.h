#pragma once

#include "quill/LogMacros.h"
#include "quill/Logger.h"

namespace hquant::v1 {

// 首次调用时启动 Quill；进程退出时等待后台日志写完。
quill::Logger* Logger();

}  // namespace hquant::v1
