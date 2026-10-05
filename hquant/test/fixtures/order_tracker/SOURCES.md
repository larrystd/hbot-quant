# Order tracker fixture sources

All cases use Python commit `9af100d6822da7d2d0291a906c730ef172284ee2`.
Order events, fill accumulation, and trade ID deduplication are drawn from
`hummingbot/connector/client_order_tracker.py` (`start_tracking_order`,
`process_trade_update`, `_process_order_update`, and event triggers) and
`hummingbot/core/data_type/in_flight_order.py` (`update_with_order_update`,
`update_with_trade_update`). The pinned unit tests in
`test/hummingbot/connector/test_client_order_tracker.py` cover creation
(309–450), fill versus completion (526–758), and completion arriving before
trade details (864–932). Fee inputs are flat quote asset fees as in that test.

The exchange status spelling in JSON follows C++ `ExchangeOrderStatus`:
`New`, `PartiallyFilled`, `Filled`, `Canceled`. Python calls `New` `OPEN`.
Stable IDs `B1`, `E1`, and `T1`/`T2` replace connector IDs. `at_us` is a
virtual input clock; Python's connector event timestamp is not compared.

Each `expected[].risk_expectation.reservation_by_asset` value is an
**architecture assertion**, not a Python `ClientOrderTracker` output.
`expectation_kind` describes only `expected[].output`; the five parity cases
compare its state, events, fills, and fee totals to Python. The fixture fixes a Buy order for 1 BTC at
100 USDT with an initial USDT reservation of 100.10, including a 0.10 fee
buffer. For these cases, verified fill of 0.4 BTC releases 40.04 USDT of the
reservation, leaving 60.06 USDT; terminal confirmation releases the rest.
This arithmetic is a fixture policy for risk integration, not a universal fee
or exchange balance rule. Python has no matching risk reservation field.

The current C++ design in `docs/README.md` and implementation in
`hquant/src/order/order_tracker.cc` use `AwaitingTrades`, `PendingCancel`,
and `SubmissionUnknown`. Python instead waits asynchronously
up to five seconds on a `FILLED` update and may emit completion with incomplete
fill data. The C++ fixture expects completion only once trade details and fees
arrive. `cancel_requested` and `submission_unknown` are C++ inputs, so those
cases are marked as intentional divergences.
