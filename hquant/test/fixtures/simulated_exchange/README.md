# Paper fixture sources

These seven cases use Hummingbot commit
`9af100d6822da7d2d0291a906c730ef172284ee2` and a fixed Binance spot
maker fee rate of `0.001`. The rate and buy fee treatment come from
`hummingbot/connector/exchange/binance/binance_utils.py:12-16`. There are no
fixed fees. All cases start with a ready `BTC-USDT` book and use quantized
limit orders. Each input's expected output is the state after local Paper
callbacks have run.

| Cases | Pinned Python source | Assertion |
| --- | --- | --- |
| `buy_limit_book_touch`, `sell_limit_book_touch` | `paper_trade_exchange.pyx:319-322,641-821,836-892` | A tick with opposite BBO equal to limit fills the entire order at its own limit price. |
| `public_sell_trade_fills_full_buy`, `public_buy_trade_fills_full_sell` | `paper_trade_exchange.pyx:113-125,190-198,894-939` | A public trade strictly past the limit fills the resting order. Equality does not. The public trade amount is `0.001`, but Python fills the full `0.01` order. **This full fill is Python parity**; the public trade is only a trigger, and its amount is not used to size the fill. A C++ partial fill model would require an intentional divergence fixture. |
| `cancel_before_touch`, `both_sides_hold_and_release` | `paper_trade_exchange.pyx:278-293,961-1011` | Resting buy holds quote, resting sell holds base. Cancellation removes the order, emits `OrderCanceled`, and restores available balance. |
| `unfunded_sell_rejected_before_exchange` | `paper_trade_exchange.pyx:385-443,747-775`; `docs/STRUCTURE_AND_TYPES.md` | Python accepts an unfunded limit sell and cancels it on attempted match. The new C++ risk admission rejects it before Paper and emits a normalized failure. This case is an intentional divergence. |

For buys, the configured percent fee is deducted from base returns. Buying
`0.01` BTC at `100` spends `1` USDT and receives `0.00999` BTC after a
`0.00001` BTC fee. For sells, the fee is deducted from quote returns. Selling
`0.01` BTC at `100` receives `0.999` USDT after a `0.001` USDT fee. The
balance calculations follow `order_candidate.py:73-80,125-145` and
`trade_fee.py:96-109,302-311`; Paper applies the calculated collateral and
returns in `paper_trade_exchange.pyx:666-730,758-821`.

Python generates random client IDs (`paper_trade_exchange.pyx:185-188`) and
timestamp based trade IDs (`:716,:807`). Fixture `id_symbols` maps them to
`B1`, `S1`, and `T1`. A public trade alone never counts as a Paper account
fill: only the match emits `OrderTraded` and changes balances.

The Cython Paper extension is not built in this checkout. These assertions
are derived from the pinned source; they have not been freshly executed
against Python in this environment.
