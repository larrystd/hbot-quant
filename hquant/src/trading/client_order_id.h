#pragma once

#include "absl/status/statusor.h"
#include "hquant/base/ids.h"

namespace hquant::v1 {

// 生成可用于交易接口的客户端订单 ID；随机源失败时不得下单。
absl::StatusOr<ClientOrderId> NewClientOrderId();

}  // namespace hquant::v1
