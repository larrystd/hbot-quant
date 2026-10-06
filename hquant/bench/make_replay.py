#!/usr/bin/env python3
"""Generate deterministic replay workloads; generation is outside timed runs."""

import argparse
import json
from pathlib import Path


MARKETS = (("BTCUSDT", "BTC"), ("ETHUSDT", "ETH"),
           ("SOLUSDT", "SOL"), ("XRPUSDT", "XRP"),
           ("BNBUSDT", "BNB"), ("ADAUSDT", "ADA"),
           ("DOGEUSDT", "DOGE"), ("LTCUSDT", "LTC"))


def write_workload(output, workload, events, shards):
    assert 1 <= shards <= len(MARKETS)
    output.mkdir(parents=True, exist_ok=True)
    inputs = []
    ordinal = 0

    def add(at_us, kind, shard, **fields):
        nonlocal ordinal
        ordinal = ordinal + 1 if inputs and at_us == inputs[-1]["at_us"] else 1
        inputs.append({"at_us": at_us, "ordinal": ordinal,
                       "kind": kind, "shard": shard, **fields})

    sequences = [11] * shards
    for shard in range(shards):
        add(0, "subscribe", shard, connection_id=1)
        add(0, "snapshot", shard, connection_id=1, last_sequence=10,
            bids=[[9999, 100]], asks=[[10001, 100]])
        add(0, "diff", shard, connection_id=1,
            first_sequence=11, last_sequence=11, bids=[], asks=[])
    for i in range(events):
        shard = i % shards
        if workload == "market":
            sequences[shard] += 1
            add(i + 1, "diff", shard, connection_id=1,
                first_sequence=sequences[shard],
                last_sequence=sequences[shard],
                bids=[[9999, 100 + i % 20]],
                asks=[[10001, 100 + i % 20]])
        elif workload == "refresh":
            add((i + 1) * 15_000_001, "timer", shard)
        else:
            at_us = (i + 1) * 1_500_001
            if i % 10 == 9:
                add(at_us, "timer", shard)
            else:
                sequences[shard] += 1
                add(at_us, "diff", shard, connection_id=1,
                    first_sequence=sequences[shard],
                    last_sequence=sequences[shard],
                    bids=[[9999, 100 + i % 20]],
                    asks=[[10001, 100 + i % 20]])

    replay = output / "replay.json"
    with replay.open("w") as stream:
        json.dump({"schema_version": 2, "inputs": inputs}, stream,
                  separators=(",", ":"))
    config = ["schema_version: 1", "input:", "  source: replay",
              f"  replay_file: {replay}", "shards:"]
    for shard, (symbol, base) in enumerate(MARKETS[:shards]):
        config += [
            f"  - id: {shard}", "    market:", f"      symbol: {symbol}",
            f"      base_asset: {base}", "      quote_asset: USDT",
            '      price_per_tick: "0.01"', '      amount_per_lot: "0.001"',
            "      stale_after_ms: 1000000000000", "      max_buffered_diffs: 1024",
            "      trading_rule:", '        price_increment: "0.01"',
            '        base_increment: "0.001"',
            '        min_base_amount: "0.001"',
            '        min_order_value: "5"', "    strategy:",
            '      order_amount: "0.1"', '      bid_spread: "0.001"',
            '      ask_spread: "0.001"', "      refresh_ms: 15000",
            "    executor:", "      mode: paper",
            f"      account: paper-{shard}", "      initial_balances:",
            f'        {base}: "0.2"', '        USDT: "2000"',
            "      hard_limits:", f'        {base}: "0.2"',
            '        USDT: "2000"', '      maker_fee_rate: "0.001"',
        ]
    (output / "config.yaml").write_text("\n".join(config) + "\n")
    print(json.dumps({"workload": workload, "shards": shards,
                      "generated_events": events, "deliveries": len(inputs),
                      "fixture_bytes": replay.stat().st_size,
                      "config": str(output / "config.yaml")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--workload", choices=("market", "refresh", "mixed"),
                        required=True)
    parser.add_argument("--events", type=int, required=True)
    parser.add_argument("--shards", type=int, default=1)
    args = parser.parse_args()
    write_workload(args.output.resolve(), args.workload, args.events,
                   args.shards)
