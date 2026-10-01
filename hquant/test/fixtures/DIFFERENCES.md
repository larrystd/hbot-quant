# Python baseline differences in fixture v1

Baseline: Hummingbot `9af100d6822da7d2d0291a906c730ef172284ee2`.
The case JSON contains the exact input, expected output, source paths, and
reason. These differences are deliberate C++ architecture rules, not claims
that the pinned Python program emits the same output.

| Area | Fixture cases | C++ rule |
| --- | --- | --- |
| Order book sequence | `duplicate_and_late_old_diff`, `gap_and_resnapshot` | Discard old updates; a sequence gap invalidates the book until a new epoch and snapshot. |
| Order book validity | `crossed_book`, `invalid_raw_precision` | Crossed books and prices that cannot map exactly to integer ticks trigger resynchronization. |
| Order book freshness | `stale_then_continuous_resume` | A stale book blocks new orders until the configured recovery condition holds. |
| Tracker cancellation | `cancel_fill_race` | Keep `cancel_pending` separate from verified fills and hold the remaining reservation until cancellation is confirmed. |
| Tracker completion | `filled_status_before_details` | Show `AwaitingFills` and defer `OrderFullyTraded` until required trade details and fees are verified. |
| Unknown submission | `submission_unknown_retains_funds_hold` | Preserve the original client ID and worst-case reservation while reconciling an uncertain write. |
| Paper admission | `unfunded_sell_rejected_before_exchange` | RiskGate rejects an unfunded order before Paper submission; Python Paper creates it and cancels at attempted match. |

Tracker fixtures that otherwise match Python put C++ reservation assertions in
`expected[i].risk_expectation`, outside the Python-parity `output`. The
`simple_pmm` fixtures capture raw strategy actions before connector
quantization. The Python strategy sends cancel requests and replacement
intents in one callback, without waiting for cancel confirmation; Dispatcher
acceptance and the Paper fill model are G1 assertions.
