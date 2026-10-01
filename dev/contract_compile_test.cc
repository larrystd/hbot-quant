#include <type_traits>

#include "hbot/base/clock.h"
#include "hbot/base/decimal.h"
#include "hbot/base/ids.h"
#include "hbot/connector/api.h"
#include "hbot/control/protocol.h"
#include "hbot/event/events.h"
#include "hbot/market_data/book_sync.h"
#include "hbot/market_data/book_view.h"
#include "hbot/model/balance.h"
#include "hbot/model/book_event.h"
#include "hbot/model/fee.h"
#include "hbot/model/market.h"
#include "hbot/model/order.h"
#include "hbot/model/trading_rule.h"
#include "hbot/net/api.h"
#include "hbot/runtime/shard_command.h"
#include "hbot/storage/ports.h"
#include "hbot/strategy/api.h"

int main() {
  static_assert(!std::is_same_v<hbot::ClientOrderId, hbot::ExchangeOrderId>);
  static_assert(!std::is_same_v<hbot::ExchangeOrderId, hbot::ExchangeTradeId>);
  auto price = hbot::Decimal::Parse("100.1");
  auto tick = hbot::Decimal::Parse("0.1");
  if (!price.ok() || !tick.ok()) return 1;
  auto rounded = price->Quantize(*tick, hbot::RoundingMode::Down);
  if (!rounded.ok() || rounded->ToString() != "100.1") return 2;
  if (hbot::Decimal::Parse("NaN").ok()) return 3;
  hbot::MarketId market{hbot::VenueId("binance"), hbot::InstrumentKind::Spot,
                        "BTCUSDT"};
  hbot::OrderRequest request;
  request.market = market;
  request.limit_price = *price;
  hbot::ActionBatch batch;
  batch.ordered.emplace_back(hbot::SubmitOrder{hbot::OwnerId{}, request});
  hbot::RecordEnvelope record;
  record.payload = hbot::OrderIntent{};
  hbot::ControlRequest control;
  control.payload = hbot::StatusRequest{};
  hbot::ShardReport report;
  return batch.ordered.size() == 1 && report.report_version == 0 ? 0 : 4;
}
