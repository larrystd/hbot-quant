# Order book fixture sources

These eight cases use the pinned Python commit
`9af100d6822da7d2d0291a906c730ef172284ee2`. They encode one market,
`BTC-USDT`, with `quote_per_tick=0.01` and `base_per_lot=0.001`. Each input has a
matching expected state, sequence, BBO, and top two levels. Prices and sizes in
level arrays are integer ticks and lots; `9990` means 99.90 and `1000` means
1.000.

| Case | Python behavior and source | C++ contract |
| --- | --- | --- |
| `pre_snapshot_buffer` | The tracker saves diffs before the book queue exists, then processes saved messages after snapshot initialization (`order_book_tracker.py:540-545,644-654`). | Buffer until snapshot 100, then replay diff 101 and enter Live. Final levels match Python. |
| `overlapping_first_diff` | Binance retains both `U` and `u` (`binance_order_book.py:45-50`); Python applies the price changes with final update ID 103 (`order_book.pyx:59-97`). | First interval 99–103 covers snapshot 100's next sequence 101. Final levels match Python. |
| `continuous_replace_and_delete` | Diff amounts replace levels; zero deletes (`order_book.pyx:69-84`). | Assert each accepted sequence, BBO, and top two levels. |
| `duplicate_and_late_old_diff` | The tracker only rejects a diff when `snapshot_uid > update_id`, then applies it (`order_book_tracker.py:555-562,652-654`). | Discard diff with `last_sequence <= current_sequence`; Python can overwrite a newer level with the old payload. |
| `gap_and_resnapshot` | `apply_diffs` has no continuity check (`order_book.pyx:59-97`). | Gap 102–103 is missing; invalidate and resynchronize in epoch 2. |
| `crossed_book` | The centralized Python book drops the older crossed level (`OrderBookEntry.cpp:61-79`). | Crossed BBO in continuous trading invalidates the book. |
| `invalid_raw_precision` | `OrderBookMessage` converts raw price/amount to `float` (`order_book_message.py:42-51`). | Price 100.005 is not divisible by tick 0.01; adapter rejects it and resynchronizes. |
| `stale_then_continuous_resume` | The Python tracker has no explicit per-book Stale state (`order_book_tracker.py:634-665`). | Silence past 5 seconds marks Stale; a contiguous diff returns to Live. |

`parity` means the accepted price levels match the pinned Python behavior. The
C++ synchronization state and integer representation are governed by
`docs/new_arch/ORDER_BOOK.md` and `STRUCTURE_AND_TYPES.md`. Cases that exercise
different acceptance or safety behavior use `intentional_divergence` and state
the reason in their JSON envelope. The Cython `order_book` extension is not
built in this checkout, so the Python observations above are traced to the
pinned source and existing `test_order_book.py`, rather than a fresh execution.
