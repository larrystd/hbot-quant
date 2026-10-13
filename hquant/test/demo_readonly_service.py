#!/usr/bin/env python3
"""Run one read-only C++ Binance Spot Demo session and check local state.

Run demo_preflight.py --private separately before this script. The config must
contain current Demo balances and max_submissions_per_run: 0.
"""

import argparse
from decimal import Decimal
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import time

from demo_preflight import read_config


SCALE = Decimal(1_000_000_000)


def control(path, operation, request_id, **fields):
    request = {"request_id": request_id, "op": operation, **fields}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(3)
        client.connect(str(path))
        client.sendall((json.dumps(request) + "\n").encode())
        data = bytearray()
        while b"\n" not in data:
            chunk = client.recv(65536)
            if not chunk:
                raise RuntimeError("control connection closed")
            data.extend(chunk)
    return json.loads(data.split(b"\n", 1)[0])


def database_checks(state):
    result = {}
    for name in ("history.sqlite", "order_journal.sqlite"):
        with sqlite3.connect(f"file:{state / name}?mode=ro&immutable=1",
                             uri=True) as db:
            if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise RuntimeError(f"{name} integrity check failed")
            if name == "history.sqlite":
                result["history_gaps"] = db.execute(
                    "SELECT COUNT(*) FROM history_gaps").fetchone()[0]
                result["order_history_events"] = db.execute(
                    "SELECT COUNT(*) FROM account_events WHERE kind IN "
                    "(1, 2, 6, 14)").fetchone()[0]
            else:
                count, low, high = db.execute(
                    "SELECT COUNT(*), MIN(sequence), MAX(sequence) FROM events"
                ).fetchone()
                result["journal_events"] = count
                result["journal_contiguous"] = (count == 0 or
                                                 (low == 1 and high == count))
                result["order_journal_events"] = db.execute(
                    "SELECT COUNT(*) FROM events WHERE kind IN "
                    "(0, 1, 2, 3, 4, 5, 6, 7, 11, 12)").fetchone()[0]
    if (result["history_gaps"] or result["order_history_events"] or
            result["order_journal_events"] or
            not result["journal_contiguous"]):
        raise RuntimeError("read-only run recorded an order or history gap")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args()
    config = args.config.resolve()
    binary = args.binary.resolve()
    state = args.state_dir.resolve()
    account_config, market, _ = read_config(config)
    if state.exists():
        raise RuntimeError("read-only check requires a new state directory")
    state.mkdir(mode=0o700, parents=True)
    log_path = state / "server.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [str(binary), f"--config={config}", f"--state_dir={state}"],
            stdout=log, stderr=subprocess.STDOUT)
        status = None
        try:
            deadline = time.monotonic() + 50
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("C++ Demo service exited during startup")
                try:
                    current = control(state / "control.sock", "status", 1)
                except (FileNotFoundError, ConnectionRefusedError):
                    time.sleep(0.25)
                    continue
                account = current["accounts"][0]
                shard = current["shards"][0]
                if (account["private_connected"] and not account["syncing"] and
                        shard["book_state"] == "live"):
                    status = current
                    break
                time.sleep(0.25)
            if status is None:
                raise RuntimeError("Demo private sync or market book timed out")
            account = status["accounts"][0]
            shard = status["shards"][0]
            if (account["ready"] or
                    account["error"] != "demo read-only validation" or
                    account["open_orders"] or account["unknown_orders"] or
                    account["queued"] or len(status["accounts"]) != 1 or
                    len(status["shards"]) != 1):
                raise RuntimeError("Demo read-only gate or account state failed")
            expected = {asset: int(Decimal(str(value)) * SCALE)
                        for asset, value in
                        account_config["initial_balances"].items()}
            actual = {asset: int(row["total_nanos"])
                      for asset, row in account["balances"].items()}
            if any(actual.get(asset) != value for asset, value in
                   expected.items()) or any(value and asset not in expected
                                           for asset, value in actual.items()):
                raise RuntimeError("C++ Demo balances differ from preflight")
            if not control(state / "control.sock", "stop", 2)["accepted"]:
                raise RuntimeError("C++ Demo stop was rejected")
            if process.wait(timeout=15) != 0:
                raise RuntimeError("C++ Demo service did not stop cleanly")
            result = {"valid": True, "symbol": market["symbol"],
                      "private_connected": account["private_connected"],
                      "book_state": shard["book_state"],
                      "applied_depth_events": shard["applied_depth_events"],
                      "accepted_trades": shard["accepted_trades"],
                      "open_orders": account["open_orders"],
                      "unknown_orders": account["unknown_orders"],
                      "balances_nanos": expected,
                      "orders_sent": 0, **database_checks(state)}
            (state / "result.json").write_text(
                json.dumps(result, indent=2) + "\n")
            print(json.dumps(result, indent=2))
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
