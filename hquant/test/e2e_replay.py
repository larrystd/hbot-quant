#!/usr/bin/env python3
"""Black-box replay checks against the real server and its SQLite history."""

import argparse
import base64
from collections import Counter, defaultdict
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import queue
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCALE = 1_000_000_000
KINDS = {
    1: "submit", 2: "cancel", 3: "accepted", 4: "filled",
    5: "cancelled", 6: "fill", 7: "balance", 8: "rejected", 9: "partial",
}
BINARY = ROOT / "bazel-bin/hquant/src/hquant_server"


def nanos(text):
    whole, _, fractional = text.partition(".")
    return int(whole) * SCALE + int(fractional.ljust(9, "0"))


def ceil_div(a, b):
    return (a + b - 1) // b


def config(replay, markets, quote_balances=None, stale_after_ms=30000):
    quote_balances = quote_balances or {}
    lines = [
        "schema_version: 1", "input:", "  source: replay",
        f"  replay_file: {replay}", "shards:",
    ]
    for shard_id, (symbol, base, price, amount, base_balance, quote_balance) in enumerate(markets):
        quote_balance = quote_balances.get(shard_id, quote_balance)
        lines += [
            f"  - id: {shard_id}", "    market:", f"      symbol: {symbol}",
            f"      base_asset: {base}", "      quote_asset: USDT",
            '      price_per_tick: "0.01"', '      amount_per_lot: "0.001"',
            f"      stale_after_ms: {stale_after_ms}",
            "      max_buffered_diffs: 1024",
            "      trading_rule:", '        price_increment: "0.01"',
            '        base_increment: "0.001"', '        min_base_amount: "0.001"',
            '        min_order_value: "5"', "    strategy:",
            f'      order_amount: "{amount}"', '      bid_spread: "0.001"',
            '      ask_spread: "0.001"', "      refresh_ms: 15000",
            "    executor:", "      mode: paper", f"      account: paper-{shard_id}",
            "      initial_balances:", f'        {base}: "{base_balance}"',
            f'        USDT: "{quote_balance}"', "      hard_limits:",
            f'        {base}: "{base_balance}"', f'        USDT: "{quote_balance}"',
            '      maker_fee_rate: "0.001"',
        ]
    return "\n".join(lines) + "\n"


class Inputs:
    def __init__(self):
        self.rows = []

    def add(self, at_us, kind, market=None, **fields):
        assert not self.rows or at_us >= self.rows[-1]["at_us"]
        ordinal = self.rows[-1]["ordinal"] + 1 if self.rows and at_us == self.rows[-1]["at_us"] else 1
        row = {"at_us": at_us, "ordinal": ordinal, "kind": kind}
        if market is not None:
            row["market"] = market
        row.update(fields)
        self.rows.append(row)

    def json(self):
        return json.dumps({"schema_version": 2, "inputs": self.rows}, separators=(",", ":"))


def run_server(fixture, markets, quote_balances=None, stale_after_ms=30000):
    assert BINARY.is_file(), f"build //hquant/src:hquant_server first: {BINARY}"
    with tempfile.TemporaryDirectory(prefix="hquant-e2e-") as directory:
        path = Path(directory)
        replay = path / "input.json"
        replay.write_text(fixture)
        configuration = path / "config.yaml"
        configuration.write_text(config(replay, markets, quote_balances,
                                         stale_after_ms))
        state = path / "state"
        result = subprocess.run(
            [str(BINARY), f"--config={configuration}", f"--state_dir={state}"],
            cwd=ROOT, capture_output=True, text=True, timeout=45,
        )
        rows, gaps = [], []
        database = state / "history.sqlite"
        if database.exists():
            with sqlite3.connect(database) as connection:
                connection.row_factory = sqlite3.Row
                rows = [dict(row) for row in connection.execute(
                    "SELECT * FROM history_records ORDER BY shard, shard_sequence"
                )]
                gaps = [dict(row) for row in connection.execute("SELECT * FROM history_gaps")]
        assert not (state / "control.sock").exists(), "control socket leaked"
        return result, rows, gaps


class MockBinance:
    """Minimal local REST snapshot and WebSocket public-stream endpoints."""

    def __init__(self):
        body = json.dumps({"lastUpdateId": 10,
                           "bids": [["99.99", "1.000"]],
                           "asks": [["100.01", "1.000"]]}).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/api/v3/depth?symbol=BTCUSDT&limit=1000":
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.http_thread = threading.Thread(target=self.http.serve_forever,
                                            daemon=True)
        self.ws = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ws.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ws.bind(("127.0.0.1", 0))
        self.ws.listen(1)
        self.ws.settimeout(0.1)
        self.ws_port = self.ws.getsockname()[1]
        self.messages = queue.Queue()
        self.connected = threading.Event()
        self.stopping = threading.Event()
        self.ws_thread = threading.Thread(target=self.serve_ws, daemon=True)
        self.client = None
        self.error = None

    def __enter__(self):
        self.http_thread.start()
        self.ws_thread.start()
        return self

    def __exit__(self, *_):
        self.stopping.set()
        if self.client:
            self.client.close()
        self.ws.close()
        self.http.shutdown()
        self.http.server_close()
        self.ws_thread.join(timeout=2)
        self.http_thread.join(timeout=2)

    def send(self, payload):
        self.messages.put(json.dumps({"stream": "btcusdt@trade", "data": payload},
                                     separators=(",", ":")).encode())

    def send_depth(self, first, last):
        self.messages.put(json.dumps({"stream": "btcusdt@depth",
                                      "data": {"e": "depthUpdate", "s": "BTCUSDT",
                                               "U": first, "u": last,
                                               "b": [], "a": []}},
                                     separators=(",", ":")).encode())

    @staticmethod
    def frame(data):
        if len(data) < 126:
            return bytes((0x81, len(data))) + data
        return bytes((0x81, 126)) + len(data).to_bytes(2, "big") + data

    def serve_ws(self):
        try:
            while not self.stopping.is_set():
                try:
                    client, _ = self.ws.accept()
                    break
                except socket.timeout:
                    continue
            else:
                return
            self.client = client
            headers = b""
            while b"\r\n\r\n" not in headers:
                headers += client.recv(4096)
            key = next(line.split(":", 1)[1].strip() for line in headers.decode().split("\r\n")
                       if line.lower().startswith("sec-websocket-key:"))
            accept = base64.b64encode(hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
            ).digest()).decode()
            client.sendall(("HTTP/1.1 101 Switching Protocols\r\n"
                            "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                            f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode())
            self.connected.set()
            self.send_depth(11, 11)
            while not self.stopping.is_set():
                try:
                    message = self.messages.get(timeout=0.1)
                except queue.Empty:
                    continue
                client.sendall(self.frame(message))
        except (OSError, StopIteration) as error:
            if not self.stopping.is_set():
                self.error = error


def control(socket_path, request):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2)
        client.connect(str(socket_path))
        client.sendall((json.dumps(request) + "\n").encode())
        data = b""
        while b"\n" not in data:
            data += client.recv(65536)
    return json.loads(data.split(b"\n", 1)[0])


def wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.02)
    raise AssertionError("timed out waiting for mock Binance state")


def reconcile(test, rows, shard, base, initial_base, initial_quote):
    """Independent integer ledger: command/report state, fills, fees, and holds."""
    active = {}
    submitted = {}
    last_fill = {}
    total = {base: nanos(initial_base), "USDT": nanos(initial_quote)}
    balance_seen = set()
    for row in rows:
        kind = KINDS[row["kind"]]
        order_id = int(row["order_id"])
        quantity = int(row["quantity_nanos"])
        price = int(row["price_nanos"])
        remaining = int(row["remaining_nanos"])
        if kind == "submit":
            test.assertEqual(row["dispatched"], 1)
            test.assertNotIn(order_id, submitted)
            submitted[order_id] = {"side": "buy" if row["buy"] else "sell",
                                   "quantity": quantity, "price": price}
        elif kind == "accepted":
            test.assertIn(order_id, submitted)
            test.assertNotIn(order_id, active)
            order = submitted[order_id].copy()
            test.assertEqual((quantity, price, remaining),
                             (order["quantity"], order["price"], order["quantity"]))
            order["remaining"] = remaining
            active[order_id] = order
        elif kind == "rejected":
            test.assertIn(order_id, submitted)
            test.assertNotIn(order_id, active)
        elif kind == "cancel":
            test.assertEqual(row["dispatched"], 1)
            test.assertIn(order_id, active)
            test.assertEqual(quantity, active[order_id]["remaining"])
        elif kind == "cancelled":
            test.assertIn(order_id, active)
            test.assertEqual(remaining, active[order_id]["remaining"])
            del active[order_id]
        elif kind == "fill":
            test.assertIn(order_id, active)
            order = active[order_id]
            test.assertGreater(quantity, 0)
            test.assertLessEqual(quantity, order["remaining"])
            gross = price * quantity // SCALE
            fee = ceil_div(gross * nanos("0.001"), SCALE)
            test.assertEqual(int(row["fee_nanos"]), fee)
            if order["side"] == "buy":
                total[base] += quantity
                total["USDT"] -= gross + fee
            else:
                total[base] -= quantity
                total["USDT"] += gross - fee
            last_fill[order_id] = quantity
        elif kind in ("partial", "filled"):
            test.assertIn(order_id, last_fill)
            test.assertIn(order_id, active)
            order = active[order_id]
            order["remaining"] -= last_fill.pop(order_id)
            test.assertEqual(remaining, order["remaining"])
            test.assertEqual(kind == "filled", remaining == 0)
            if kind == "filled":
                del active[order_id]
        elif kind == "balance":
            asset = row["asset"]
            test.assertIn(asset, total)
            test.assertEqual(int(row["total_nanos"]), total[asset])
            frozen = 0
            for order in active.values():
                if (asset == "USDT") != (order["side"] == "buy"):
                    continue
                if order["side"] == "sell":
                    frozen += order["remaining"]
                else:
                    gross = ceil_div(order["price"] * order["remaining"], SCALE)
                    fee = ceil_div(gross * nanos("0.001"), SCALE)
                    frozen += gross + fee + order["remaining"] // nanos("0.001")
            test.assertEqual(int(row["available_nanos"]), total[asset] - frozen)
            balance_seen.add(asset)
    test.assertFalse(active, f"shard {shard} has active orders after shutdown")
    test.assertFalse(last_fill)
    test.assertEqual(balance_seen, {base, "USDT"})
    return total


class ReplayE2E(unittest.TestCase):
    def test_repeated_full_snapshot_reprices_orders(self):
        market = [("BTCUSDT", "BTC", "100", "0.1", "0.2", "2000")]
        events = Inputs()
        events.add(0, "subscribe", connection_id=1)
        events.add(0, "snapshot", connection_id=1, last_sequence=10,
                   bids=[[9999, 100]], asks=[[10001, 100]])
        events.add(15_000_000, "snapshot", connection_id=1,
                   last_sequence=11, bids=[[9998, 100]], asks=[[10000, 100]])
        result, rows, gaps = run_server(events.json(), market)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(gaps, [])
        submits = defaultdict(list)
        for row in rows:
            if row["kind"] == 1:
                submits[row["at_us"]].append(int(row["price_nanos"]))
        self.assertEqual(sorted(submits[0]), [nanos("99.90"), nanos("100.10")])
        self.assertEqual(sorted(submits[15_000_000]),
                         [nanos("99.89"), nanos("100.09")])

    def test_complex_two_shard(self):
        markets = [
            ("BTCUSDT", "BTC", "100", "0.1", "0.2", "2000"),
            ("ETHUSDT", "ETH", "200", "0.05", "0.1", "1000"),
        ]
        events = Inputs()
        for market in ("BTCUSDT", "ETHUSDT"):
            events.add(0, "subscribe", market, connection_id=1)
        for market, bid, ask in (("BTCUSDT", 9999, 10001),
                                 ("ETHUSDT", 19999, 20001)):
            events.add(0, "snapshot", market, connection_id=1,
                       last_sequence=10, bids=[[bid, 100]], asks=[[ask, 100]])
            events.add(0, "diff", market, connection_id=1,
                       first_sequence=11, last_sequence=11, bids=[], asks=[])
        events.add(1, "public_trade", "BTCUSDT", price_ticks=10100,
                   quantity_lots=30, side="buy")
        events.add(2, "public_trade", "BTCUSDT", price_ticks=10100,
                   quantity_lots=70, side="buy")
        events.add(3, "public_trade", "BTCUSDT", price_ticks=9800,
                   quantity_lots=40, side="sell")
        events.add(4, "diff", "BTCUSDT", connection_id=1,
                   first_sequence=12, last_sequence=12,
                   bids=[[9999, 0], [9970, 100]],
                   asks=[[10001, 0], [9980, 60]])
        events.add(5, "public_trade", "ETHUSDT", price_ticks=19800,
                   quantity_lots=20, side="sell")
        events.add(15_000_001, "timer", "BTCUSDT")
        events.add(15_000_002, "diff", "BTCUSDT", connection_id=1,
                   first_sequence=14, last_sequence=14, bids=[], asks=[])
        events.add(15_000_003, "diff", "BTCUSDT", connection_id=1,
                   first_sequence=13, last_sequence=13, bids=[], asks=[])
        events.add(15_000_004, "subscribe", "BTCUSDT", connection_id=2)
        events.add(15_000_005, "diff", "BTCUSDT", connection_id=2,
                   first_sequence=21, last_sequence=21, bids=[], asks=[])
        events.add(15_000_006, "snapshot", "BTCUSDT", connection_id=2,
                   last_sequence=20, bids=[[9999, 100]], asks=[[10001, 100]])
        events.add(31_000_000, "timer", "ETHUSDT")
        events.add(31_000_001, "diff", "ETHUSDT", connection_id=1,
                   first_sequence=12, last_sequence=12, bids=[], asks=[])
        events.add(31_000_002, "subscribe", "ETHUSDT", connection_id=2)
        events.add(31_000_003, "diff", "ETHUSDT", connection_id=2,
                   first_sequence=21, last_sequence=21, bids=[], asks=[])
        events.add(31_000_004, "snapshot", "ETHUSDT", connection_id=2,
                   last_sequence=20, bids=[[19999, 100]], asks=[[20001, 100]])
        result, rows, gaps = run_server(events.json(), markets)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(gaps, [])
        by_shard = defaultdict(list)
        for row in rows:
            by_shard[row["shard"]].append(row)
        self.assertEqual(set(by_shard), {0, 1})
        for shard, records in by_shard.items():
            self.assertEqual([r["shard_sequence"] for r in records],
                             list(range(1, len(records) + 1)))
            self.assertEqual(len({r["run_id"] for r in records}), 1)
            self.assertEqual([r["at_us"] for r in records],
                             sorted(r["at_us"] for r in records))
        btc = reconcile(self, by_shard[0], 0, "BTC", "0.2", "2000")
        eth = reconcile(self, by_shard[1], 1, "ETH", "0.1", "1000")
        self.assertEqual(btc, {"BTC": nanos("0.2"), "USDT": nanos("2000.006006")})
        self.assertEqual(eth, {"ETH": nanos("0.12"), "USDT": nanos("996.000004")})
        btc_fills = [(int(r["order_id"]), int(r["price_nanos"]),
                      int(r["quantity_nanos"])) for r in by_shard[0]
                     if r["kind"] == 6]
        self.assertEqual(btc_fills, [
            (2, nanos("100.10"), nanos("0.03")),
            (2, nanos("100.10"), nanos("0.07")),
            (1, nanos("99.90"), nanos("0.04")),
            (1, nanos("99.80"), nanos("0.06")),
        ])
        eth_fills = [(int(r["price_nanos"]), int(r["quantity_nanos"]))
                     for r in by_shard[1] if r["kind"] == 6]
        self.assertEqual(eth_fills, [(nanos("199.80"), nanos("0.02"))])
        self.assertEqual(Counter(r["kind"] for r in by_shard[0])[1], 6)
        self.assertEqual(Counter(r["kind"] for r in by_shard[1])[1], 4)
        self.assertEqual(Counter(r["kind"] for r in by_shard[0])[5], 4)
        self.assertEqual(Counter(r["kind"] for r in by_shard[1])[5], 4)

    def test_exchange_reject_is_separate_from_dispatch(self):
        market = [("BTCUSDT", "BTC", "100", "0.1", "0.1", "9.99999")]
        events = Inputs()
        events.add(0, "subscribe", connection_id=1)
        events.add(0, "snapshot", connection_id=1, last_sequence=10,
                   bids=[[9999, 100]], asks=[[10001, 100]])
        events.add(0, "diff", connection_id=1, first_sequence=11,
                   last_sequence=11, bids=[], asks=[])
        result, rows, gaps = run_server(events.json(), market)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(gaps, [])
        buys = [r for r in rows if r["kind"] == 1 and r["buy"]]
        self.assertEqual(len(buys), 1)
        self.assertEqual(buys[0]["dispatched"], 1)
        self.assertEqual(len([r for r in rows if r["kind"] == 8 and
                              r["order_id"] == buys[0]["order_id"]]), 1)
        self.assertFalse([r for r in rows if r["kind"] == 3 and
                          r["order_id"] == buys[0]["order_id"]])
        self.assertFalse([r for r in rows if r["kind"] == 6])
        self.assertEqual(Counter(r["kind"] for r in rows)[5], 1)

    def test_refresh_history_remains_complete_under_backpressure(self):
        market = [("BTCUSDT", "BTC", "100", "0.1", "0.2", "2000")]
        events = Inputs()
        events.add(0, "subscribe", connection_id=1)
        events.add(0, "snapshot", connection_id=1, last_sequence=10,
                   bids=[[9999, 100]], asks=[[10001, 100]])
        events.add(0, "diff", connection_id=1, first_sequence=11,
                   last_sequence=11, bids=[], asks=[])
        refreshes = 3000
        for i in range(refreshes):
            events.add((i + 1) * 15_000_001, "timer")
        result, rows, gaps = run_server(events.json(), market,
                                        stale_after_ms=1_000_000_000_000)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(gaps, [])
        self.assertEqual(len(rows), 6 + 14 * refreshes + 6)
        self.assertEqual([r["shard_sequence"] for r in rows],
                         list(range(1, len(rows) + 1)))
        self.assertEqual(Counter(r["kind"] for r in rows)[1],
                         2 * (refreshes + 1))

    def test_local_binance_wire_to_control_and_history(self):
        market = [("BTCUSDT", "BTC", "100", "0.1", "0.2", "2000")]
        with tempfile.TemporaryDirectory(prefix="hquant-wire-") as directory, MockBinance() as mock:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            configuration = root / "config.yaml"
            text = config("unused", market).replace(
                "  source: replay\n  replay_file: unused\n",
                "  source: binance_public\n  rest_host: 127.0.0.1\n"
                "  websocket_host: 127.0.0.1\n"
                f"  rest_port: {mock.http.server_port}\n"
                f"  websocket_port: {mock.ws_port}\n  tls: false\n")
            configuration.write_text(text)
            process = subprocess.Popen(
                [str(BINARY), f"--config={configuration}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
            )
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)

                def status():
                    answer = control(socket_path, {"op": "status", "request_id": 1})
                    self.assertTrue(answer["ok"], answer)
                    return answer["shards"][0]

                ready = wait_until(lambda: (value if (value := status())["active_orders"] == 2
                                           and value["book_state"] == "live" else None))
                self.assertEqual(ready["base_total_nanos"], str(nanos("0.2")))
                self.assertTrue(mock.connected.is_set())
                changed = control(socket_path, {
                    "op": "set_strategy", "request_id": 4, "shard_id": 0,
                    "expected_version": 1,
                    "patch": {"bid_spread": "0.002", "ask_spread": "0.002",
                              "refresh_ms": 12000},
                })
                self.assertTrue(changed["ok"], changed)
                self.assertEqual((changed["config_version"], changed["actions"]),
                                 (2, 4))
                stale = control(socket_path, {
                    "op": "set_strategy", "request_id": 5, "shard_id": 0,
                    "expected_version": 1, "patch": {"bid_spread": "0.003"},
                })
                self.assertFalse(stale["ok"])
                self.assertIn("version mismatch", stale["error"])
                updated = wait_until(lambda: (value if (value := status())["config_version"] == 2
                                              and value["active_orders"] == 2 else None))
                self.assertEqual(updated["book_state"], "live")
                for trade_id, quantity in ((101, "0.030"), (102, "0.070")):
                    mock.send({"e": "trade", "s": "BTCUSDT", "p": "101.00",
                               "q": quantity, "t": trade_id, "m": False})
                filled = wait_until(lambda: (value if (value := status())["base_total_nanos"]
                                             == str(nanos("0.1")) else None))
                self.assertEqual(filled["quote_total_nanos"], str(nanos("2010.00998")))
                mock.send_depth(13, 13)  # sequence 12 is missing
                unavailable = wait_until(lambda: (value if (value := status())["book_state"]
                                                  == "syncing" and value["active_orders"] == 0
                                                  else None))
                self.assertIn("depth", unavailable["last_error"])
                history = wait_until(lambda: (value if len((value := control(
                    socket_path, {"op": "history", "request_id": 2,
                                  "limit": 100}))["records"]) >= 16 else None))
                self.assertTrue(history["ok"])
                self.assertEqual(control(socket_path, {"op": "stop", "request_id": 3})
                                 ["accepted"], True)
                stdout, stderr = process.communicate(timeout=8)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                self.assertIsNone(mock.error)
                self.assertFalse(socket_path.exists())
                with sqlite3.connect(state / "history.sqlite") as db:
                    db.row_factory = sqlite3.Row
                    rows = [dict(row) for row in db.execute(
                        "SELECT * FROM history_records ORDER BY shard_sequence")]
                    gaps = db.execute("SELECT COUNT(*) FROM history_gaps").fetchone()[0]
                self.assertEqual(gaps, 0)
                totals = reconcile(self, rows, 0, "BTC", "0.2", "2000")
                self.assertEqual(totals, {"BTC": nanos("0.1"),
                                          "USDT": nanos("2010.00998")})
                self.assertEqual([int(row["price_nanos"]) for row in rows
                                  if row["kind"] == 6],
                                 [nanos("100.20"), nanos("100.20")])
                self.assertEqual([int(row["quantity_nanos"]) for row in rows
                                  if row["kind"] == 6],
                                 [nanos("0.03"), nanos("0.07")])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_out_of_order_replay_fails_cleanly(self):
        market = [("BTCUSDT", "BTC", "100", "0.1", "0.2", "2000")]
        events = Inputs()
        events.add(2, "subscribe", connection_id=1)
        malformed = json.loads(events.json())
        malformed["inputs"].append({"at_us": 1, "ordinal": 1,
                                    "kind": "timer"})
        result, _, gaps = run_server(json.dumps(malformed), market)
        self.assertEqual(result.returncode, 1)
        self.assertIn("not strictly ordered", result.stderr)
        self.assertEqual(gaps, [])

    def test_excess_precision_rejected_before_start(self):
        market = [("BTCUSDT", "BTC", "100", "0.1000000001", "0.2", "2000")]
        result, rows, gaps = run_server('{"schema_version":1,"inputs":[]}', market)
        self.assertEqual(result.returncode, 2)
        self.assertEqual((rows, gaps), ([], []))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=BINARY)
    arguments, unittest_args = parser.parse_known_args()
    BINARY = arguments.binary.resolve()
    unittest.main(argv=[__file__, *unittest_args])
