#!/usr/bin/env python3
"""Generate deterministic full-depth equity replay cases for 1000 symbols."""

import argparse
import json
from collections import Counter
from pathlib import Path


STOCKS = 1000
LEVELS_PER_SIDE = 10
CASES = {
    "nyse": {"duration_seconds": 180, "book_interval_us": 1_000_000,
             "trade_interval_us": 10_000_000, "quote": "USD",
             "base_price_ticks": 10_000, "order_amount": "1",
             "base_increment": "1"},
    "sse": {"duration_seconds": 180, "book_interval_us": 3_000_000,
            "trade_interval_us": 30_000_000, "quote": "CNY",
            "base_price_ticks": 3_000, "order_amount": "100",
            "base_increment": "100"},
    "stress": {"duration_seconds": 30, "book_interval_us": 125_000,
               "trade_interval_us": 500_000, "quote": "USD",
               "base_price_ticks": 10_000, "order_amount": "1",
               "base_increment": "1"},
}


def symbol(name, shard):
    return f"{name.upper()}_SIM_{shard + 1:04d}"


def book_levels(case, shard, slot, at_us):
    midpoint = (case["base_price_ticks"] + shard * 5 +
                (at_us // 30_000_000) % 2)
    bids = [[midpoint - 1 - rank,
             1000 + 10 * ((slot + shard + rank) % 20)]
            for rank in range(LEVELS_PER_SIDE)]
    asks = [[midpoint + 1 + rank,
             1000 + 10 * ((slot + shard + rank + 3) % 20)]
            for rank in range(LEVELS_PER_SIDE)]
    return bids, asks


class Writer:
    def __init__(self, output):
        self.stream = (output / "replay.json").open("w")
        self.stream.write('{"schema_version":2,"inputs":[')
        self.first = True
        self.last_at = -1
        self.ordinal = 0
        self.kinds = Counter()
        self.per_shard = Counter()
        self.level_rows = 0
        self.buckets = {1_000: Counter(), 100_000: Counter(),
                        1_000_000: Counter()}

    def add(self, at_us, kind, shard, **fields):
        if at_us < self.last_at:
            raise ValueError("event times must be nondecreasing")
        self.ordinal = self.ordinal + 1 if at_us == self.last_at else 1
        self.last_at = at_us
        event = {"at_us": at_us, "ordinal": self.ordinal,
                 "kind": kind, "shard": shard, **fields}
        if not self.first:
            self.stream.write(",")
        json.dump(event, self.stream, separators=(",", ":"))
        self.first = False
        self.kinds[kind] += 1
        self.per_shard[shard] += 1
        self.level_rows += len(fields.get("bids", ())) + len(fields.get("asks", ()))
        for span, bucket in self.buckets.items():
            bucket[at_us // span] += 1

    def close(self):
        self.stream.write("]}\n")
        self.stream.close()
        return {
            "deliveries": sum(self.kinds.values()),
            "event_counts": dict(sorted(self.kinds.items())),
            "per_shard_deliveries": [self.per_shard[i] for i in range(STOCKS)],
            "level_rows": self.level_rows,
            "scheduled_peak_1ms": max(self.buckets[1_000].values()),
            "scheduled_peak_100ms": max(self.buckets[100_000].values()),
            "scheduled_peak_1s": max(self.buckets[1_000_000].values()),
        }


def generate_inputs(writer, name, case):
    duration_us = case["duration_seconds"] * 1_000_000
    books = [None] * STOCKS
    sequences = [9] * STOCKS
    trade_ids = [0] * STOCKS
    for shard in range(STOCKS):
        writer.add(0, "subscribe", shard, market=symbol(name, shard),
                   connection_id=1)

    schedule = []
    book_phase_step = min(100, case["book_interval_us"] // STOCKS)
    for shard in range(STOCKS):
        for slot in range(duration_us // case["book_interval_us"]):
            at_us = slot * case["book_interval_us"] + shard * book_phase_step
            schedule.append((at_us, 0, shard, slot))
        for slot in range(duration_us // case["trade_interval_us"]):
            at_us = (slot * case["trade_interval_us"] +
                     shard * case["trade_interval_us"] // STOCKS)
            schedule.append((at_us, 1, shard, slot))

    for at_us, kind, shard, slot in sorted(schedule):
        market = symbol(name, shard)
        if kind == 0:
            bids, asks = book_levels(case, shard, slot, at_us)
            books[shard] = (bids, asks)
            sequences[shard] += 1
            writer.add(at_us, "snapshot", shard, market=market,
                       connection_id=1, last_sequence=sequences[shard],
                       bids=bids, asks=asks)
        else:
            trade_ids[shard] += 1
            side = "buy" if trade_ids[shard] % 2 else "sell"
            price = (books[shard][1][0][0] if side == "buy"
                     else books[shard][0][0][0])
            writer.add(at_us, "public_trade", shard, market=market,
                       connection_id=1, trade_id=trade_ids[shard],
                       price_ticks=price, quantity_lots=100, side=side)
    return books, sequences, trade_ids


def write_config(output, name, case):
    lines = ["schema_version: 1", "input:", "  source: replay",
             f"  replay_file: {output / 'replay.json'}", "shards:"]
    for shard in range(STOCKS):
        market = symbol(name, shard)
        lines.extend([
            f"  - id: {shard}", "    market:", f"      symbol: {market}",
            f"      base_asset: {market}", f"      quote_asset: {case['quote']}",
            '      price_per_tick: "0.01"', '      amount_per_lot: "1"',
            "      stale_after_ms: 35000", "      max_buffered_diffs: 1024",
            "      trading_rule:", '        price_increment: "0.01"',
            f'        base_increment: "{case["base_increment"]}"',
            f'        min_base_amount: "{case["base_increment"]}"',
            '        min_order_value: "1"', "    strategy:",
            f'      order_amount: "{case["order_amount"]}"',
            '      bid_spread: "0.001"', '      ask_spread: "0.001"',
            "      refresh_ms: 15000", "    executor:", "      mode: paper",
            f"      account: paper-{name}-{shard}", "      initial_balances:",
            f'        {market}: "1000"', f'        {case["quote"]}: "1000000"',
            "      hard_limits:", f'        {market}: "1000"',
            f'        {case["quote"]}: "1000000"',
            '      maker_fee_rate: "0.001"',
        ])
    (output / "config.yaml").write_text("\n".join(lines) + "\n")


def make_case(output, name, book_interval_us=None, trade_interval_us=None):
    case = dict(CASES[name])
    if book_interval_us is not None:
        case["book_interval_us"] = book_interval_us
    if trade_interval_us is not None:
        case["trade_interval_us"] = trade_interval_us
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    writer = Writer(output)
    books, sequences, trade_ids = generate_inputs(writer, name, case)
    metrics = writer.close()
    write_config(output, name, case)
    duration_us = case["duration_seconds"] * 1_000_000
    book_messages = STOCKS * (duration_us // case["book_interval_us"])
    trades = STOCKS * (duration_us // case["trade_interval_us"])
    manifest = {
        "schema_version": 1, "case": name,
        "provenance": "synthetic full-depth book and public trades; counts are test choices",
        "stocks": STOCKS, "levels_per_side": LEVELS_PER_SIDE,
        "book_messages_including_initial_snapshots": book_messages,
        "public_trades": trades, "duration_us": duration_us,
        "book_interval_us": case["book_interval_us"],
        "trade_interval_us": case["trade_interval_us"],
        "replay_is_paced": False,
        "final_sequences": sequences, "final_trade_ids": trade_ids,
        "final_best_bid_ask": [
            {"bid": bids[0], "ask": asks[0]} for bids, asks in books
        ],
        **metrics,
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--case", choices=(*CASES, "all"), required=True)
    parser.add_argument("--stress-book-interval-us", type=int)
    parser.add_argument("--stress-trade-interval-us", type=int)
    args = parser.parse_args()
    if (args.stress_book_interval_us is not None or
            args.stress_trade_interval_us is not None):
        if args.case != "stress":
            parser.error("custom intervals require --case stress")
        if (args.stress_book_interval_us is None or
                args.stress_trade_interval_us is None or
                args.stress_book_interval_us < STOCKS or
                args.stress_trade_interval_us < STOCKS):
            parser.error("both stress intervals must be at least 1000 us")
    for name in CASES if args.case == "all" else (args.case,):
        directory = args.output / name if args.case == "all" else args.output
        result = make_case(directory, name, args.stress_book_interval_us,
                           args.stress_trade_interval_us)
        print(json.dumps({key: result[key] for key in
                          ("case", "stocks", "levels_per_side",
                           "book_messages_including_initial_snapshots",
                           "public_trades", "deliveries")}))
