#!/usr/bin/env python3
"""Report client-observed stages for one instrumented Binance Spot Demo order."""

import argparse
import csv
import json
from collections import defaultdict, deque
from pathlib import Path
import re
import sqlite3


SHARD_TRACE = re.compile(
    r"shard_submit_trace client_id=(\S+) requested_ns=(\d+) "
    r"id_ready_ns=(\d+) started_ns=(\d+) finished_ns=(\d+)"
)
REPORT_STATUS = {"NEW": "2", "PARTIALLY_FILLED": "3",
                 "FILLED": "4", "CANCELED": "5"}


def elapsed_us(begin, end):
    begin, end = int(begin), int(end)
    if begin <= 0 or end < begin:
        raise ValueError(f"invalid monotonic timestamps: {begin}, {end}")
    return round((end - begin) / 1000, 3)


def rows(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def build_report(state, client_id):
    state = Path(state)
    probe = json.loads((state / "probe.json").read_text())
    matches = [match for match in SHARD_TRACE.finditer(
        (state / "server.log").read_text())
        if match.group(1) == client_id]
    if len(matches) != 1:
        raise ValueError(f"expected one Shard submit trace for {client_id}")
    requested, id_ready, admitted, returned = map(int, matches[0].groups()[1:])
    rest = [row for row in rows(state / "rest_client_trace.csv")
            if row["client_id"] == client_id]
    submits = [row for row in rest if row["kind"] == "SUBMIT"]
    cancels = [row for row in rest if row["kind"] == "CANCEL"]
    if len(submits) != 1:
        raise ValueError(f"expected one REST submit trace for {client_id}")
    submit = submits[0]
    if "response_done_ns" not in submit:
        raise ValueError("REST trace lacks response_done_ns")

    def rest_stages(row):
        queued = int(row["enqueued_ns"])
        dispatched = int(row["dispatched_ns"])
        writing = int(row["write_start_ns"])
        written = int(row["write_done_ns"])
        response = int(row["response_done_ns"])
        stages = {
            "queue_us": elapsed_us(queued, dispatched),
            "dispatch_to_write_us": elapsed_us(dispatched, writing),
            "request_write_us": elapsed_us(writing, written),
            "write_to_http_response_us": elapsed_us(written, response),
            "enqueue_to_http_response_us": elapsed_us(queued, response),
        }
        if int(row["first_write_call_ns"]) and int(row["first_write_return_ns"]):
            stages["first_async_write_call_us"] = elapsed_us(
                row["first_write_call_ns"], row["first_write_return_ns"])
        return {"kind": row["kind"], "stages_us": stages}

    with sqlite3.connect(
            f"file:{state / 'history.sqlite'}?mode=ro&immutable=1",
            uri=True) as db:
        db.row_factory = sqlite3.Row
        history = [dict(row) for row in db.execute(
            "SELECT kind, message, at_mono_ns FROM account_events "
            "WHERE client_order_id=? AND kind IN (6, 14) "
            "ORDER BY account_sequence", (client_id,))]
    applied = defaultdict(deque)
    for row in history:
        key = row["message"] if row["kind"] == 14 else "FILL"
        applied[key].append(row["at_mono_ns"])
    private = []
    for row in rows(state / "private_client_trace.csv"):
        if row["client_id"] != client_id or row["kind"] not in ("ORDER", "FILL"):
            continue
        status = row["status"] if row["kind"] == "ORDER" else "FILL"
        key = REPORT_STATUS.get(status) if row["kind"] == "ORDER" else "FILL"
        if not applied[key]:
            raise ValueError(f"private {status} has no matching local apply record")
        frame = int(row["frame_complete_ns"])
        parsed = int(row["parsed_ns"])
        callback = int(row["callback_done_ns"])
        applied_ns = applied[key].popleft()
        origin = cancels[0] if status == "CANCELED" and cancels else submit
        entry = {
            "status": status,
            "rest_kind": origin["kind"],
            "write_start_to_frame_us": elapsed_us(origin["write_start_ns"], frame),
            "frame_to_parse_us": elapsed_us(frame, parsed),
            "parse_to_callback_done_us": elapsed_us(parsed, callback),
            "frame_to_account_record_us": elapsed_us(frame, applied_ns),
        }
        first_read = int(row["first_socket_read_ns"])
        if first_read:
            entry["first_socket_read_to_frame_us"] = elapsed_us(first_read, frame)
        if status == "NEW":
            entry["order_request_to_account_record_us"] = elapsed_us(
                requested, applied_ns)
        private.append(entry)
    if not any(row["status"] == "NEW" for row in private):
        raise ValueError("no NEW private order report")
    if any(applied.values()):
        raise ValueError("local private apply records lack matching WS events")

    market = {}
    for key in ("read_to_parse", "parse_to_market", "live_market",
                "untagged_submit_strategy"):
        sample = probe.get(key, {})
        if sample.get("count", 0):
            market[key] = {name: sample[name] for name in
                           ("count", "p50_us", "p99_us")}
    return {
        "client_order_id": client_id,
        "clock": "client CLOCK_MONOTONIC",
        "order_stages_us": {
            "client_order_id": elapsed_us(requested, id_ready),
            "shard_admission": elapsed_us(id_ready, admitted),
            "admission_to_rest_enqueue": elapsed_us(admitted,
                                                    submit["enqueued_ns"]),
            "submit_function_return": elapsed_us(admitted, returned),
            "order_request_to_http_response": elapsed_us(
                requested, submit["response_done_ns"]),
        },
        "rest_requests": [rest_stages(row) for row in rest],
        "private_reports": private,
        "public_market_samples": market,
        "limits": [
            "No Binance server receipt timestamp; HTTP response wait includes network and venue processing.",
            "Demo public market frames have no bench_seq to join one frame to this order.",
            "Write-start-to-FILLED or CANCELED frame includes market wait or cancellation, not only transport.",
            "Private frame-to-account-record ends when the local account records the applied event; history SQLite commit is later.",
            "Market frame distributions sample every tenth frame; one order is not a percentile sample.",
        ],
    }


def print_report(report):
    print("Demo client latency (microseconds; one order):")
    for name, value in report["order_stages_us"].items():
        print(f"  {name}: {value:.3f}")
    for request in report["rest_requests"]:
        print(f"  REST {request['kind']}:")
        for name, value in request["stages_us"].items():
            print(f"    {name}: {value:.3f}")
    for event in report["private_reports"]:
        print(f"  private {event['status']} (REST {event['rest_kind']}):")
        for name, value in event.items():
            if name not in ("status", "rest_kind"):
                print(f"    {name}: {value:.3f}")
    for name, sample in report["public_market_samples"].items():
        print(f"  market {name}: n={sample['count']} "
              f"p50={sample['p50_us']:.3f} p99={sample['p99_us']:.3f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--client-id", required=True)
    args = parser.parse_args()
    report = build_report(args.state_dir, args.client_id)
    (args.state_dir / "latency.json").write_text(
        json.dumps(report, indent=2) + "\n")
    print_report(report)


if __name__ == "__main__":
    main()
