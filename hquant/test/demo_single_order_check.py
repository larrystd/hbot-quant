#!/usr/bin/env python3
"""Place at most one Binance Spot Demo order, settle it, and reconcile it.

Run demo_preflight.py --private with the same read-only config first. This
script writes a temporary max_submissions_per_run: 1 config into a new state
directory and removes it after the session.
"""

import argparse
from decimal import Decimal
import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yaml

from demo_preflight import read_config, read_credentials, signed_get
from demo_readonly_service import SCALE, control
from demo_latency_report import build_report, print_report


def nanos(value):
    return int(Decimal(str(value)) * SCALE)


def emergency_cancel(symbol, client_id, credentials):
    params = {"symbol": symbol, "origClientOrderId": client_id,
              "recvWindow": 5000, "timestamp": int(time.time() * 1000)}
    payload = urlencode(params)
    signature = hmac.new(credentials["api_secret"].encode(),
                         payload.encode(), hashlib.sha256).hexdigest()
    request = Request("https://demo-api.binance.com/api/v3/order?" +
                      payload + "&signature=" + signature,
                      headers={"X-MBX-APIKEY": credentials["api_key"]},
                      method="DELETE")
    with urlopen(request, timeout=8) as response:
        return json.load(response)["status"]


def history_rows(state):
    path = state / "history.sqlite"
    if not path.exists():
        return []
    with sqlite3.connect(str(path), timeout=2) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(
            "SELECT * FROM account_events ORDER BY account_sequence")]


def check_sqlite(state, client):
    with sqlite3.connect(f"file:{state / 'history.sqlite'}?immutable=1",
                         uri=True) as db:
        db.row_factory = sqlite3.Row
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok" or \
                db.execute("SELECT COUNT(*) FROM history_gaps").fetchone()[0]:
            raise RuntimeError("history SQLite integrity or gap check failed")
        events = [dict(row) for row in db.execute(
            "SELECT * FROM account_events ORDER BY account_sequence")]
    sequences = [row["account_sequence"] for row in events]
    if sequences != list(range(1, len(sequences) + 1)):
        raise RuntimeError("account event sequence is not contiguous")
    submits = [row for row in events if row["kind"] == 1]
    reports = [row for row in events if row["kind"] == 14 and
               row["client_order_id"] == client]
    fills = [row for row in events if row["kind"] == 6 and
             row["client_order_id"] == client]
    cancels = [row for row in events if row["kind"] == 2 and
               row["client_order_id"] == client]
    if (len(submits) != 1 or submits[0]["client_order_id"] != client or
            len([row for row in reports if row["message"] == "2"]) != 1 or
            len([row for row in reports if row["message"] in ("4", "5")]) != 1 or
            len([row for row in reports if row["message"] in ("3", "4")]) !=
            len(fills) or
            any(int(row["trade_id"]) == 0 for row in fills) or
            len({row["trade_id"] for row in fills}) != len(fills) or
            len(cancels) > 1):
        raise RuntimeError("local Demo order events did not settle once")
    terminal = next(row["message"] for row in reports
                    if row["message"] in ("4", "5"))
    remaining = next(int(row["remaining_nanos"]) for row in reports
                     if row["message"] == terminal)
    if remaining != int(submits[0]["quantity_nanos"]) - sum(
            int(row["quantity_nanos"]) for row in fills):
        raise RuntimeError("Demo terminal remaining quantity differs from fills")
    if (terminal == "5" and len(cancels) != 1) or \
            (terminal == "4" and sum(int(row["quantity_nanos"])
                                     for row in fills) !=
             int(submits[0]["quantity_nanos"])):
        raise RuntimeError("Demo cancel/fill terminal state is inconsistent")
    with sqlite3.connect(f"file:{state / 'order_journal.sqlite'}?immutable=1",
                         uri=True) as db:
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("journal SQLite integrity check failed")
        count, low, high = db.execute(
            "SELECT COUNT(*), MIN(sequence), MAX(sequence) FROM events"
        ).fetchone()
        intents = db.execute(
            "SELECT COUNT(*) FROM events WHERE kind=0").fetchone()[0]
        journal_fills = db.execute(
            "SELECT COUNT(*) FROM events WHERE kind=7").fetchone()[0]
        if (low != 1 or high != count or intents != 1 or
                journal_fills != len(fills)):
            raise RuntimeError("journal intent/fill count is inconsistent")
    return submits[0], fills, terminal, len(cancels), len(events), count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readonly-config", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--observe-seconds", type=float, default=2)
    args = parser.parse_args()
    if not 0 <= args.observe_seconds <= 120:
        raise ValueError("observe-seconds must be between 0 and 120")
    readonly = args.readonly_config.resolve()
    binary = args.binary.resolve()
    state = args.state_dir.resolve()
    account, market, config_dir = read_config(readonly)
    if state.exists():
        raise RuntimeError("single-order check requires a new state directory")
    credentials = read_credentials(account, config_dir)
    state.mkdir(mode=0o700, parents=True)
    config_path = state / "order_config.yaml"
    config = yaml.safe_load(readonly.read_text())
    config["accounts"][0]["max_submissions_per_run"] = 1
    credentials_path = Path(account["api_credentials_file"])
    if not credentials_path.is_absolute():
        credentials_path = (config_dir / credentials_path).resolve()
    config["accounts"][0]["api_credentials_file"] = str(credentials_path)
    fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as out:
        yaml.safe_dump(config, out, sort_keys=False)
    client_id = None
    process = None
    final_status = None
    verified = False
    try:
        with (state / "server.log").open("w") as log:
            process = subprocess.Popen(
                [str(binary), f"--config={config_path}",
                 f"--state_dir={state}",
                 f"--bench_output={state / 'probe.json'}"],
                stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("Demo service exited before first order")
                submits = [row for row in history_rows(state)
                           if row["kind"] == 1]
                if submits:
                    if len(submits) != 1:
                        raise RuntimeError("Demo sent more than one order")
                    client_id = submits[0]["client_order_id"]
                    break
                time.sleep(0.25)
            if not client_id:
                raise RuntimeError("Demo strategy did not submit an order")
            observe_deadline = time.monotonic() + args.observe_seconds
            while time.monotonic() < observe_deadline:
                if any(row["kind"] == 14 and
                       row["client_order_id"] == client_id and
                       row["message"] == "4" for row in history_rows(state)):
                    break
                time.sleep(max(0.0, min(0.25,
                                        observe_deadline - time.monotonic())))
            response = control(state / "control.sock", "cancel_all", 1,
                               account_id=account["id"], operator="test",
                               reason="single-order Demo correctness cleanup")
            if not response.get("accepted"):
                raise RuntimeError("Demo cancel_all was rejected")
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("Demo service exited before settlement")
                current = control(state / "control.sock", "status", 2)
                current_account = current["accounts"][0]
                terminal = any(row["kind"] == 14 and
                               row["client_order_id"] == client_id and
                               row["message"] in ("4", "5")
                               for row in history_rows(state))
                if (terminal and current_account["open_orders"] == 0 and
                        current_account["unknown_orders"] == 0 and
                        current_account["pending_journal"] == 0):
                    final_status = current_account
                    break
                time.sleep(0.25)
            if final_status is None:
                raise RuntimeError("Demo order did not reach a clean terminal state")
            time.sleep(1)
            final_status = control(state / "control.sock", "status", 6)[
                "accounts"][0]
            if (final_status["open_orders"] or final_status["unknown_orders"] or
                    final_status["pending_journal"]):
                raise RuntimeError("Demo account reopened after terminal report")
            if not control(state / "control.sock", "stop", 3).get("accepted"):
                raise RuntimeError("Demo service stop was rejected")
            if process.wait(timeout=15) != 0:
                raise RuntimeError("Demo service exited unsuccessfully")
        submit, fills, terminal, cancels, history_count, journal_count = \
            check_sqlite(state, client_id)
        latency_report = None
        latency_error = None
        try:
            latency_report = build_report(state, client_id)
            (state / "latency.json").write_text(
                json.dumps(latency_report, indent=2) + "\n")
        except (OSError, ValueError, KeyError, sqlite3.Error) as error:
            latency_error = str(error)
        # Give the Demo request budget a full interval after service sync.
        time.sleep(60)
        symbol = market["symbol"]
        order = signed_get("/api/v3/order",
                           {"symbol": symbol, "origClientOrderId": client_id},
                           credentials, recv_window_ms=10000)
        trades = signed_get("/api/v3/myTrades",
                            {"symbol": symbol, "orderId": order["orderId"]},
                            credentials, recv_window_ms=10000)
        open_orders = signed_get("/api/v3/openOrders", {}, credentials,
                                 recv_window_ms=10000)
        remote_account = signed_get("/api/v3/account",
                                    {"omitZeroBalances": "false"}, credentials,
                                    recv_window_ms=10000)
        expected_terminal = "FILLED" if terminal == "4" else "CANCELED"
        if (order["clientOrderId"] != client_id or
                str(order["orderId"]) != next(
                    row["exchange_order_id"] for row in history_rows(state)
                    if row["kind"] == 14 and row["client_order_id"] == client_id
                    and row["exchange_order_id"] != "0") or
                order["status"] != expected_terminal or open_orders or
                nanos(order["origQty"]) != int(submit["quantity_nanos"]) or
                nanos(order["executedQty"]) != sum(
                    int(row["quantity_nanos"]) for row in fills)):
            raise RuntimeError("Demo order status differs from local history")
        local_trades = {int(row["trade_id"]): row for row in fills}
        if {int(trade["id"]) for trade in trades} != set(local_trades):
            raise RuntimeError("Demo trade IDs differ from local fills")
        for trade in trades:
            local = local_trades[int(trade["id"])]
            if (nanos(trade["price"]) != int(local["price_nanos"]) or
                    nanos(trade["qty"]) != int(local["quantity_nanos"]) or
                    nanos(trade["commission"]) != int(local["fee_nanos"]) or
                    trade["commissionAsset"] != local["asset"]):
                raise RuntimeError("Demo trade quantity, price or fee differs")
        remote_balances = {row["asset"]: nanos(row["free"]) +
                           nanos(row["locked"])
                           for row in remote_account["balances"]}
        local_balances = {asset: int(row["total_nanos"])
                          for asset, row in final_status["balances"].items()}
        if any(remote_balances.get(asset, 0) != total for asset, total in
               local_balances.items()) or any(total and asset not in
                                              local_balances for asset, total
                                              in remote_balances.items()):
            raise RuntimeError("Demo account balances differ from C++ state")
        result = {"valid": True, "symbol": symbol,
                  "client_order_id": client_id,
                  "exchange_order_id": order["orderId"],
                  "terminal_status": expected_terminal,
                  "fill_count": len(fills), "cancel_count": cancels,
                  "open_orders": len(open_orders),
                  "unknown_orders": final_status["unknown_orders"],
                  "history_events": history_count,
                  "journal_events": journal_count,
                  "signed_verification_gets": 4,
                  "orders_sent": 1,
                  "final_balances_nanos": local_balances}
        if latency_report is not None:
            result["latency_report"] = "latency.json"
        else:
            result["latency_error"] = latency_error
        (state / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
        if latency_report is not None:
            print_report(latency_report)
        verified = True
    finally:
        if process is not None and process.poll() is None:
            try:
                control(state / "control.sock", "cancel_all", 4,
                        account_id=account["id"], operator="test",
                        reason="single-order Demo failure cleanup")
                control(state / "control.sock", "stop", 5)
                process.wait(timeout=10)
            except (OSError, KeyError, subprocess.TimeoutExpired):
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        if client_id and not verified:
            try:
                open_orders = signed_get("/api/v3/openOrders", {}, credentials,
                                         recv_window_ms=10000)
                if any(row["clientOrderId"] == client_id for row in open_orders):
                    status = emergency_cancel(market["symbol"], client_id,
                                              credentials)
                    print("Emergency Demo cancel status: " + status,
                          file=sys.stderr)
            except Exception as error:
                print("WARNING: could not verify Demo order cleanup: " +
                      type(error).__name__, file=sys.stderr)
        config_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
