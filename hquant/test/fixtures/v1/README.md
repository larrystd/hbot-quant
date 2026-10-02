# Fixture v1: deterministic behavior cases

Fixture JSON lives under `hquant/test/fixtures/`. In `source_files`, Python
paths refer to the pinned Python repository; `docs/...` paths refer to this
C++ repository's design documents. These source references did not move with
the C++ fixture files.

Every case is one JSON object. The C++ fixture loader validates this envelope and
the family-specific event/output discriminators. A fixture may document a Python
behavior (`parity`) or a deliberate new-architecture rule
(`intentional_divergence`). Do not label a rule that Python does not implement as
`parity`.

```json
{
  "schema_version": 1,
  "family": "order_book",
  "case_id": "order_book/overlapping_first_diff",
  "baseline_commit": "9af100d6822da7d2d0291a906c730ef172284ee2",
  "source_files": ["hummingbot/core/data_type/order_book.pyx"],
  "expectation_kind": "parity",
  "divergence_reason": null,
  "setup": {},
  "inputs": [{"at_us": 0, "ordinal": 0, "event": {"kind": "subscribe"}}],
  "expected": [{"at_us": 0, "ordinal": 0, "output": {}}]
}
```

`family` is `order_book`, `order_tracker`, or `simple_pmm` in G0, and `paper`
from G1 onward.
`case_id` starts with `family + "/"` and is unique across all files. The
`source_files` paths cite the pinned Python repository or this repository's
`docs/` directory. A divergence requires a
nonempty `divergence_reason`; parity uses `null`. `inputs` and `expected` have
the same nonzero length. Entry *i* in each array has the same `(at_us, ordinal)`.
Both are nonnegative integers; pairs increase lexicographically, and `ordinal`
orders simultaneous inputs. `event` and `output` are objects. For Tracker
cases, optional `expected[i].risk_expectation` is a separate architecture
assertion; `expectation_kind` describes `output` only. All money, price,
amount, balance, and fee values are decimal **strings**; book ticks/lots and
sequence values are JSON integers. Exchange-provided random IDs are replaced
consistently by stable symbols such as `B1`, `S1`, `T1`; an explicit
`id_symbols` object may record that mapping. No credentials or real account IDs.

## `order_book`

`setup` fixes `market`, `quote_per_tick`, `base_per_lot`, `scale_version`, and
`stream_epoch`. `event.kind` is `subscribe`, `snapshot`, `diff`, `disconnect`,
`timer`, or `invalid_raw_level`. Snapshot has `last_sequence`; diff has
`first_sequence` and `last_sequence`. Both have `bids` and `asks`, arrays of
`[price_ticks, quantity_lots]`; zero lots in a diff deletes that level.
`invalid_raw_level` has `side`, `raw_price`, `raw_quantity`, and `reason` to
exercise adapter rejection before the order book. All events may include
`stream_epoch`; snapshot/diff must include it. `output` has:

```json
{"state":"Buffering","last_sequence":null,"applied":false,
 "reason":"None","bbo":{"bid":null,"ask":null},
 "top_bids":[],"top_asks":[]}
```

`state` is `Subscribing`, `Buffering`, `Replaying`, `Live`, `Stale`, or
`Resyncing`. `reason` is `None`, `OldDiff`, `Gap`, `CrossedBook`,
`InvalidScale`, `InvalidMessage`, `BufferOverflow`, `Disconnected`, or
`Expired`. BBO and top levels use `[price_ticks, quantity_lots]` or `null` for
an absent best. `applied` means this input changed accepted book state; an old
diff does not count. An `invalid_raw_level` output represents adapter rejection
and must not silently alter accepted levels.

## `order_tracker`

`setup` fixes `account`, `market`, numeric `strategy_id` (a positive 48-bit
integer), `client_order_id`, `side`,
`quantity`, `limit_price`, and optional `initial_reservation_by_asset`.
`event.kind` is `register`, `order_update`, `trade_update`,
`cancel_requested`, `submission_unknown`, or `reconcile`. Order updates carry
`exchange_status`, optional `exchange_order_id`, and optional cumulative
decimal strings. Trade updates carry `trade_id`, `price`, `quantity`,
`value`, `fees_by_asset`, and at least one order ID. Inputs that only
the C++ architecture supports are `intentional_divergence`. `output` has:

```json
{"display_state":"PendingCreate","events":[],
 "traded_quantity":"0","traded_value":"0","fees_by_asset":{},
 "remaining_base":"1"}
```

`events` is the ordered list emitted *by this input*, with values
`OrderOpened`, `OrderTraded`, `OrderFullyTraded`, `OrderCanceled`, or
`OrderFailed`. `display_state` is `Absent` or a value of `OrderDisplayState`.
`remaining_base` is the requested amount minus verified fills.
If present, `expected[i].risk_expectation` is
`{"reservation_by_asset":{"USDT":"100.10"}}`. It is an architecture
assertion based on the fixture's explicit initial reservation and fee buffer,
not a claim of Python parity. Keep Python-parity Tracker outputs marked
`parity` even when this separate risk assertion is present.

## `simple_pmm`

`setup` fixes `account`, `market`, numeric `strategy_id` (a positive 48-bit
integer), and `config` containing
`order_amount`, `bid_spread`, `ask_spread`, `refresh_interval_us`, and
`price_type` (`mid` or `last`). A `tick` event supplies `ready`, `mid_price`,
optional `last_price`, `active_orders` in their Python iteration order, and
`available_balances` as decimal strings by asset. Other event kinds are not
part of this first strategy fixture. `output` has `actions` (an ordered array)
and `next_refresh_at_us` (integer or null). Each action is either
`{"kind":"cancel","client_order_id":"B1"}` or
`{"kind":"submit","side":"Buy","price":"99.9","amount":"0.01"}`.
The strategy output is before connector/Paper quantization. If Python sends a
zero-amount candidate because its budget checker adjusted that side to zero,
record the raw action here; G1 Dispatcher tests cover C++ rejection. The first
tick that changes `ready` from false to true only
initializes `StrategyV2Base`; `on_tick` starts with the following tick. At
refresh, cancel requests precede new submit intents in the same callback;
there is no wait for cancel confirmation.

## `paper` (G1)

`setup` fixes `market`, `base_asset`, `quote_asset`, numeric `strategy_id`,
`initial_balances` (asset → decimal string), `maker_fee_rate` (decimal string),
`buy_fee_from_returns` (boolean), `price_increment`, and `base_increment`
(decimal strings). The fee setup is explicit so fixtures never depend on a
live connector's current fee schedule. `event.kind` is `submit`, `cancel`,
`book_bbo`, `public_trade`, or `tick`:

- `submit`: `client_order_id`, `side` (`Buy`/`Sell`), `price`, `amount` as decimal
  strings. The ID is a stable fixture symbol. Inputs are already quantized
  unless a case explicitly tests connector quantization.
- `cancel`: `client_order_id`.
- `book_bbo`: `bid` and `ask` as decimal strings, then process crossed orders.
- `public_trade`: `side`, `price`, `amount`; this is an external market trade,
  distinct from the Paper account fill. Python's limit-order matcher uses the
  price crossing and fills the whole resting order even if public trade amount
  is smaller; a C++ partial-fill model must mark that case as a divergence.
- `tick`: no further fields; advances Paper's queued work at the given time.

Each `output` is the state **after** processing the input and flushing local
Paper event callbacks. It has ordered `events` emitted by this input, each an
object with `kind` (`OrderOpened`, `OrderTraded`, `OrderFullyTraded`,
`OrderCanceled`, `OrderFailed`) and `client_order_id`; an `OrderTraded` additionally
has `trade_id`, `price`, `amount`, and `fee_by_asset` (asset → decimal string).
It also has `open_orders`, ordered by fixture ID, each with `client_order_id`,
`side`, `price`, and `amount`; `balances` and `available_balances` as asset →
decimal string maps; and cumulative `fees_by_asset`. The account fill event,
balances, and fee map must agree. Normalize random Python order/trade IDs via
`id_symbols` in the case. RiskGate admission is outside the Paper-only case;
where Python accepts an unfunded limit order but C++ rejects it before Paper,
mark the case as `intentional_divergence` and state the boundary.
