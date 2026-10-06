#!/usr/bin/env python3
"""Independently validate all symbols and messages in a market replay case."""

import argparse
import json
from collections import Counter
from pathlib import Path


def validate(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    replay = json.loads((directory / "replay.json").read_text())
    assert replay["schema_version"] == 2
    stocks = manifest["stocks"]
    depth = manifest["levels_per_side"]
    book_interval = manifest["book_interval_us"]
    trade_interval = manifest["trade_interval_us"]
    duration = manifest["duration_us"]
    name = manifest["case"].upper()
    connected = [False] * stocks
    books = [None] * stocks
    sequences = [None] * stocks
    trade_ids = [0] * stocks
    snapshot_counts = [0] * stocks
    counts = Counter()
    per_shard = Counter()
    buckets = {1_000: Counter(), 100_000: Counter(), 1_000_000: Counter()}
    level_rows = 0
    previous_time = (-1, 0)
    for event in replay["inputs"]:
        at_us, ordinal = event["at_us"], event["ordinal"]
        assert (at_us, ordinal) > previous_time
        assert 0 <= at_us < duration
        assert ordinal == (previous_time[1] + 1 if at_us == previous_time[0]
                           else 1)
        previous_time = (at_us, ordinal)
        shard, kind = event["shard"], event["kind"]
        assert 0 <= shard < stocks
        assert event["market"] == f"{name}_SIM_{shard + 1:04d}"
        assert event["connection_id"] == 1
        counts[kind] += 1
        per_shard[shard] += 1
        for span, bucket in buckets.items():
            bucket[at_us // span] += 1
        if kind == "subscribe":
            assert at_us == 0 and not connected[shard]
            connected[shard] = True
            continue
        assert connected[shard]
        if kind == "snapshot":
            index = snapshot_counts[shard]
            assert at_us == (index * book_interval +
                             shard * min(100, book_interval // stocks))
            assert event["last_sequence"] == 10 + index
            assert sequences[shard] is None or event["last_sequence"] == sequences[shard] + 1
            bids, asks = event["bids"], event["asks"]
            assert len(bids) == len(asks) == depth
            assert all(price > 0 and quantity > 0 for price, quantity in bids + asks)
            assert all(bids[i][0] > bids[i + 1][0] for i in range(depth - 1))
            assert all(asks[i][0] < asks[i + 1][0] for i in range(depth - 1))
            assert bids[0][0] < asks[0][0]
            level_rows += len(bids) + len(asks)
            books[shard] = (bids, asks)
            sequences[shard] = event["last_sequence"]
            snapshot_counts[shard] += 1
        else:
            assert kind == "public_trade" and books[shard] is not None
            index = trade_ids[shard]
            assert at_us == (index * trade_interval +
                             shard * trade_interval // stocks)
            assert event["trade_id"] == index + 1
            assert event["quantity_lots"] == 100
            side = "buy" if index % 2 == 0 else "sell"
            assert event["side"] == side
            assert event["price_ticks"] == (
                books[shard][1][0][0] if side == "buy"
                else books[shard][0][0][0])
            trade_ids[shard] += 1

    book_per_stock = duration // book_interval
    trades_per_stock = duration // trade_interval
    assert all(connected)
    assert snapshot_counts == [book_per_stock] * stocks
    assert trade_ids == [trades_per_stock] * stocks
    assert counts == {"subscribe": stocks,
                      "snapshot": stocks * book_per_stock,
                      "public_trade": stocks * trades_per_stock}
    assert dict(sorted(counts.items())) == manifest["event_counts"]
    assert sum(counts.values()) == manifest["deliveries"]
    assert [per_shard[i] for i in range(stocks)] == manifest[
        "per_shard_deliveries"]
    assert counts["snapshot"] == manifest[
        "book_messages_including_initial_snapshots"]
    assert counts["public_trade"] == manifest["public_trades"]
    assert level_rows == manifest["level_rows"] == 2 * depth * counts["snapshot"]
    assert sequences == manifest["final_sequences"]
    assert trade_ids == manifest["final_trade_ids"]
    for span, key in ((1_000, "scheduled_peak_1ms"),
                      (100_000, "scheduled_peak_100ms"),
                      (1_000_000, "scheduled_peak_1s")):
        assert max(buckets[span].values()) == manifest[key]
    for shard, (bids, asks) in enumerate(books):
        assert manifest["final_best_bid_ask"][shard] == {
            "bid": bids[0], "ask": asks[0]}
    return {"case": manifest["case"], "stocks": stocks,
            "deliveries": sum(counts.values()), "level_rows": level_rows}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(validate(args.directory)))
