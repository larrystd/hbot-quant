#!/usr/bin/env python3
"""Black-box signed REST and private WS trade-chain test."""

import argparse
import asyncio
from collections import Counter
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "hquant/bench"))
sys.path.insert(0, str(ROOT / "hquant/test"))
from venue.mock_binance_server import MockBinanceTrading  # noqa: E402
from mock_public_server import MockBinance, control, wait_until  # noqa: E402


BINARY = ROOT / "bazel-bin/hquant/src/hquant_server"


class TradingThread:
    def __init__(self, fill_delay=0.05, balances=None, fee_asset=None):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.mock = None
        self.fill_delay = fill_delay
        self.balances = balances or {"BTC": "0", "USDT": "1000"}
        self.fee_asset = fee_asset

    def __enter__(self):
        self.thread.start()
        async def create():
            self.mock = MockBinanceTrading({"BTCUSDT": ("BTC", "USDT")},
                                           self.balances,
                                           fill_delay=self.fill_delay,
                                           fee_asset=self.fee_asset)
            return await self.mock.start()
        self.ports = asyncio.run_coroutine_threadsafe(
            create(), self.loop).result(timeout=5)
        return self

    def __exit__(self, *_):
        asyncio.run_coroutine_threadsafe(self.mock.close(), self.loop).result(timeout=5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()

    def wait_ready(self):
        future = asyncio.run_coroutine_threadsafe(
            asyncio.wait_for(self.mock.ws_ready.wait(), 5), self.loop)
        future.result(timeout=6)

    def snapshot(self):
        async def read():
            return {**self.mock.snapshot(),
                    "private_connections": self.mock.private_connections,
                    "rate_limited_at_ns": self.mock.rate_limited_at_ns}
        return asyncio.run_coroutine_threadsafe(read(), self.loop).result(timeout=2)

    def disconnect_private(self):
        asyncio.run_coroutine_threadsafe(
            self.mock.disconnect_private(), self.loop).result(timeout=3)

    def suppress_next_submit_and_private_events(self):
        async def set_faults():
            self.mock.drop_submit_response_once = True
            self.mock.suppress_private_events = True
        asyncio.run_coroutine_threadsafe(set_faults(), self.loop).result(timeout=2)

    def set_rule(self, symbol, field, value):
        async def change():
            self.mock.rules[symbol][field] = value
        asyncio.run_coroutine_threadsafe(change(), self.loop).result(timeout=2)

    def rate_limit_next_submit(self):
        async def change():
            self.mock.rate_limit_submit_once = True
        asyncio.run_coroutine_threadsafe(change(), self.loop).result(timeout=2)

    def timeout_submit_and_cancel_without_private_events(self):
        async def change():
            self.mock.timeout_submit_response_once = True
            self.mock.timeout_cancel_without_execution_once = True
            self.mock.suppress_private_events = True
        asyncio.run_coroutine_threadsafe(change(), self.loop).result(timeout=2)

    def inject_silent_fills(self, client_id, count, quantity_nanos):
        async def fill():
            self.mock.suppress_private_events = True
            order = self.mock.orders[client_id]
            async with self.mock.order_locks[client_id]:
                for _ in range(count):
                    await self.mock._fill(order, quantity_nanos)
        asyncio.run_coroutine_threadsafe(fill(), self.loop).result(timeout=15)

    def inject_external_debit(self, asset, amount_nanos):
        async def debit():
            self.mock.wallet[asset]["free"] -= amount_nanos
            await self.mock._balances([asset])
        asyncio.run_coroutine_threadsafe(debit(), self.loop).result(timeout=3)


def configuration(public, trading):
    rest_port, ws_port = public
    trading_rest, private_ws = trading
    return f'''schema_version: 2
input:
  source: binance_public
  rest_host: 127.0.0.1
  rest_port: {rest_port}
  websocket_host: 127.0.0.1
  websocket_port: {ws_port}
  tls: false
accounts:
  - id: shared
    mode: pressure_sim
    venue: binance_spot
    trading_rest:
      host: 127.0.0.1
      port: {trading_rest}
      tls: false
    private_ws:
      host: 127.0.0.1
      port: {private_ws}
      tls: false
    initial_balances:
      BTC: "0"
      USDT: "1000"
    hard_limits:
      BTC: "1000"
      USDT: "1000"
    maker_fee_rate: "0.001"
shards:
  - id: 0
    account_id: shared
    market:
      symbol: BTCUSDT
      base_asset: BTC
      quote_asset: USDT
      price_per_tick: "0.01"
      amount_per_lot: "0.001"
      stale_after_ms: 30000
      max_buffered_diffs: 1024
      trading_rule:
        price_increment: "0.01"
        base_increment: "0.001"
        min_base_amount: "0.001"
        min_order_value: "5"
    strategy:
      order_amount: "0.1"
      bid_spread: "0.001"
      ask_spread: "0.001"
      refresh_ms: 15000
'''


class TradeE2E(unittest.TestCase):
    def test_external_balance_change_pauses_before_new_order(self):
        with tempfile.TemporaryDirectory(prefix="hquant-ext-bal-") as directory, \
                MockBinance() as public, TradingThread(fill_delay=30) as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                def account():
                    return control(socket_path, {"op": "status",
                                                 "request_id": 1})["accounts"][0]
                wait_until(lambda: account()["ready"])
                trading.inject_external_debit("USDT", 10_000_000_000)
                wait_until(lambda: not account()["ready"] and
                           "private balance requires reconciliation" in
                           account()["error"])
                self.assertTrue(control(socket_path, {
                    "op": "sync_account_state", "request_id": 2,
                    "account_id": "shared", "operator": "test",
                    "reason": "verify external balance change"})["accepted"])
                wait_until(lambda: not account()["syncing"] and
                           "unexplained account balance difference: USDT" in
                           account()["error"])
                public.send_depth(12, 12)
                time.sleep(0.2)
                self.assertFalse([row for row in trading.snapshot()["received"]
                                  if row["kind"] == "submit"])
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 3})["accepted"])
                process.communicate(timeout=12)
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=3)

    def test_third_asset_fee_matches_venue_and_sqlite(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-third-fee-") as directory, \
                MockBinance() as public, TradingThread(
                    balances={"BTC": "0", "USDT": "1000", "BNB": "1"},
                    fee_asset="BNB") as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            text = configuration(
                (public.http.server_port, public.ws_port), trading.ports)
            config.write_text(text.replace("    initial_balances:\n",
                                           "    initial_balances:\n"
                                           "      BNB: \"1\"\n", 1))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                def account():
                    return control(socket_path, {"op": "status",
                                                 "request_id": 1})["accounts"][0]
                wait_until(lambda: account()["ready"])
                public.send_depth(12, 12)
                wait_until(lambda: trading.snapshot()["trades"])
                self.assertTrue(control(socket_path, {
                    "op": "sync_account_state", "request_id": 2,
                    "account_id": "shared", "operator": "test",
                    "reason": "verify third-asset fee"})["accepted"])
                wait_until(lambda: account()["ready"] and
                           not account()["syncing"], timeout=10)
                venue = trading.snapshot()
                self.assertEqual(venue["trades"][0]["fee_asset"], "BNB")
                self.assertEqual(int(account()["balances"]["BNB"]
                                     ["total_nanos"]),
                                 venue["wallet"]["BNB"]["free"])
                self.assertEqual(venue["wallet"]["BNB"]["free"], 999_990_000)
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 3})["accepted"])
                stdout, stderr = process.communicate(timeout=12)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                with sqlite3.connect(state / "history.sqlite") as db:
                    row = db.execute(
                        "SELECT asset, fee_nanos FROM account_events "
                        "WHERE kind=6 ORDER BY id LIMIT 1").fetchone()
                self.assertEqual((row[0], int(row[1])), ("BNB", 10_000))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_changed_balance_seed_is_rejected_after_fill_replay(self):
        with tempfile.TemporaryDirectory(prefix="hquant-seed-") as directory, \
                MockBinance() as public, TradingThread() as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            original = configuration(
                (public.http.server_port, public.ws_port), trading.ports)
            config.write_text(original)
            command = [str(BINARY), f"--config={config}",
                       f"--state_dir={state}"]
            process = subprocess.Popen(command, cwd=ROOT,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                public.send_depth(12, 12)
                wait_until(lambda: trading.snapshot()["trades"])
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 2})["accepted"])
                stdout, stderr = process.communicate(timeout=20)
                self.assertEqual(process.returncode, 0, stdout + stderr)

                wallet = trading.snapshot()["wallet"]
                def amount(asset):
                    total = wallet[asset]["free"] + wallet[asset]["locked"]
                    return f"{total // 1_000_000_000}.{total % 1_000_000_000:09d}"
                config.write_text(original.replace(
                    '      BTC: "0"\n', f'      BTC: "{amount("BTC")}"\n', 1
                ).replace(
                    '      USDT: "1000"\n',
                    f'      USDT: "{amount("USDT")}"\n', 1))
                submitted = len([row for row in trading.snapshot()["received"]
                                 if row["kind"] == "submit"])
                process = subprocess.Popen(command, cwd=ROOT,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, text=True)
                def account_error():
                    try:
                        return control(socket_path, {
                            "op": "status", "request_id": 3
                        })["accounts"][0]["error"]
                    except (OSError, ValueError):
                        return ""
                wait_until(lambda: "unexplained account balance difference"
                           in account_error(), timeout=12)
                self.assertEqual(submitted, len([
                    row for row in trading.snapshot()["received"]
                    if row["kind"] == "submit"]))
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 4})["accepted"])
                process.communicate(timeout=20)
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=3)

    def test_ambiguous_submit_and_cancel_resolve_without_duplicate_submit(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-unknown-") as directory, \
                MockBinance() as public, TradingThread(fill_delay=30) as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports
            ).replace("refresh_ms: 15000", "refresh_ms: 1000"))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                trading.timeout_submit_and_cancel_without_private_events()
                public.send_depth(12, 12)
                first = wait_until(lambda: next((row for row in
                    trading.snapshot()["received"] if row["kind"] == "submit"), None))
                client_id = first["client_id"]
                try:
                    wait_until(lambda: len([row for row in
                        trading.snapshot()["received"] if row["kind"] == "cancel"
                        and row["client_id"] == client_id]) >= 2,
                        timeout=12)
                except AssertionError:
                    status = control(socket_path, {"op": "status",
                                                   "request_id": 30})
                    account = status["accounts"][0]
                    self.fail(f"cancel did not retry: "
                              f"{ {key: account[key] for key in ('ready', 'syncing', 'unknown_orders', 'error')} }, "
                              f"shard={status['shards'][0]}, "
                              f"venue={trading.snapshot()['orders'].get(client_id)}, "
                              f"requests={[row['kind'] for row in trading.snapshot()['received']]}")
                wait_until(lambda: trading.snapshot()["orders"][client_id]
                           ["status"] == "CANCELED", timeout=5)
                self.assertEqual(len([row for row in
                    trading.snapshot()["received"] if row["kind"] == "submit"
                    and row["client_id"] == client_id]), 1)
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 2})["accepted"])
                stdout, stderr = process.communicate(timeout=20)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                with sqlite3.connect(state / "history.sqlite") as db:
                    kinds = {row[0] for row in db.execute(
                        "SELECT kind FROM account_events WHERE client_order_id=?",
                        (client_id,))}
                self.assertTrue({3, 5, 10, 11}.issubset(kinds), kinds)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_journal_write_failure_pauses_new_orders(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-journal-fault-") as directory, \
                MockBinance() as public, TradingThread() as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                with sqlite3.connect(state / "order_journal.sqlite") as db:
                    db.execute(
                        "CREATE TRIGGER fail_journal BEFORE INSERT ON events "
                        "BEGIN SELECT RAISE(FAIL, 'injected write failure'); END")
                public.send_depth(12, 12)
                wait_until(lambda: "journal" in
                           control(socket_path, {"op": "status", "request_id": 2})
                           ["accounts"][0]["error"])
                wait_until(lambda: any(
                    row["kind"] == "submit"
                    for row in trading.snapshot()["received"]))
                time.sleep(.2)
                submitted = len([row for row in trading.snapshot()["received"]
                                 if row["kind"] == "submit"])
                public.send_depth(13, 13)
                time.sleep(.2)
                self.assertEqual(submitted, len([
                    row for row in trading.snapshot()["received"]
                    if row["kind"] == "submit"]))
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 3})["accepted"])
                stdout, stderr = process.communicate(timeout=38)
                self.assertNotEqual(process.returncode, 0, stdout + stderr)
                self.assertIn("submit intent journal failed", stderr)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_trade_query_paginates_and_deduplicates(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-pages-") as directory, \
                MockBinance() as public, TradingThread(fill_delay=30) as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                public.send_depth(12, 12)
                first = wait_until(lambda: next((row for row in
                    trading.snapshot()["received"] if row["kind"] == "submit"), None))
                trading.inject_silent_fills(first["client_id"], 1001, 50_000)
                self.assertTrue(control(socket_path, {
                    "op": "sync_account_state", "request_id": 2,
                    "account_id": "shared", "operator": "test",
                    "reason": "recover paginated fills"})["accepted"])
                wait_until(lambda: (account := control(socket_path, {
                    "op": "status", "request_id": 3})["accounts"][0])
                    ["ready"] and account["open_orders"] == 1,
                    timeout=20)
                self.assertTrue(control(socket_path, {
                    "op": "sync_account_state", "request_id": 4,
                    "account_id": "shared", "operator": "test",
                    "reason": "verify fill deduplication"})["accepted"])
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 5})["accounts"][0]["ready"],
                    timeout=20)
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 6})["accepted"])
                stdout, stderr = process.communicate(timeout=20)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                with sqlite3.connect(state / "history.sqlite") as db:
                    self.assertEqual(db.execute(
                        "SELECT COUNT(*) FROM account_events WHERE kind=6"
                    ).fetchone()[0], 1001)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_rate_limit_unknown_order_stays_reserved_across_restart(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-429-") as directory, \
                MockBinance() as public, TradingThread() as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            command = [str(BINARY), f"--config={config}",
                       f"--state_dir={state}"]
            process = subprocess.Popen(command, cwd=ROOT,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                trading.rate_limit_next_submit()
                public.send_depth(12, 12)
                wait_until(lambda: (account := control(socket_path, {
                    "op": "status", "request_id": 2})["accounts"][0])
                    ["unknown_orders"] == 1 and account["open_orders"] == 1,
                    timeout=8)
                query = wait_until(lambda: next((row for row in
                    trading.snapshot()["received"] if row["kind"] == "query"), None),
                    timeout=8)
                self.assertGreaterEqual(query["at_ns"] -
                                        trading.snapshot()["rate_limited_at_ns"],
                                        900_000_000)
                self.assertFalse([row for row in trading.snapshot()["received"]
                                  if row["kind"] == "submit"])
                process.kill()
                process.communicate(timeout=3)
                process = subprocess.Popen(command, cwd=ROOT,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, text=True)
                def still_unknown():
                    try:
                        account = control(socket_path, {
                            "op": "status", "request_id": 3})["accounts"][0]
                        return account["unknown_orders"] == 1 and not account["ready"]
                    except (OSError, ValueError):
                        return False
                wait_until(still_unknown, timeout=10)
                self.assertFalse([row for row in trading.snapshot()["received"]
                                  if row["kind"] == "submit"])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_rule_change_pauses_new_orders(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-rules-") as directory, \
                MockBinance() as public, TradingThread() as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                trading.set_rule("BTCUSDT", "price_increment", "0.04")
                self.assertTrue(control(socket_path, {
                    "op": "sync_account_state", "request_id": 20,
                    "account_id": "shared", "operator": "test",
                    "reason": "compatible venue rule changed"})["accepted"])
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 21})["accounts"][0]["ready"])
                public.send_depth(12, 12)
                first = wait_until(lambda: next((row for row in
                    trading.snapshot()["received"] if row["kind"] == "submit"), None))
                self.assertEqual(first["price"] % 40_000_000, 0)
                trading.set_rule("BTCUSDT", "price_increment", "0.025")
                self.assertTrue(control(socket_path, {
                    "op": "sync_account_state", "request_id": 2,
                    "account_id": "shared", "operator": "test",
                    "reason": "venue rule changed"})["accepted"])
                wait_until(lambda: "current venue rule" in control(socket_path, {
                    "op": "status", "request_id": 3})["accounts"][0]["error"])
                public.send_depth(13, 13)
                time.sleep(.15)
                self.assertEqual(len([row for row in trading.snapshot()["received"]
                                      if row["kind"] == "submit"]), 1)
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 4})["accepted"])
                stdout, stderr = process.communicate(timeout=12)
                self.assertEqual(process.returncode, 0, stdout + stderr)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_lost_submit_response_and_private_events_recover_by_query(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-query-") as directory, \
                MockBinance() as public, TradingThread() as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                trading.suppress_next_submit_and_private_events()
                public.send_depth(12, 12)
                first = wait_until(lambda: next((row for row in
                    trading.snapshot()["received"] if row["kind"] == "submit"), None))
                wait_until(lambda: trading.snapshot()["trades"])
                synced = control(socket_path, {
                    "op": "sync_account_state", "request_id": 2,
                    "account_id": "shared", "operator": "test",
                    "reason": "recover missing response and private fill"})
                self.assertTrue(synced["accepted"], synced)
                try:
                    wait_until(lambda: (account := control(socket_path, {
                        "op": "status", "request_id": 3})["accounts"][0])
                        ["ready"] and account["open_orders"] == 1 and
                        account["unknown_orders"] == 0,
                        timeout=10)
                except AssertionError:
                    account = control(socket_path, {
                        "op": "status", "request_id": 30})["accounts"][0]
                    self.fail(f"query recovery stalled: "
                              f"{ {key: account[key] for key in ('ready', 'syncing', 'open_orders', 'error')} }, "
                              f"venue={trading.snapshot()['orders']}")
                self.assertEqual([row["client_id"] for row in
                                  trading.snapshot()["received"]
                                  if row["kind"] == "submit"],
                                 [first["client_id"]])
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 4})["accepted"])
                stdout, stderr = process.communicate(timeout=12)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                with sqlite3.connect(state / "history.sqlite") as db:
                    self.assertEqual(db.execute(
                        "SELECT COUNT(*) FROM account_events WHERE kind=6"
                    ).fetchone()[0], len(trading.snapshot()["trades"]))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_account_control_pause_resume_and_cancel_all(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-control-") as directory, \
                MockBinance() as public, TradingThread(fill_delay=30) as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                paused = control(socket_path, {
                    "op": "pause_new_orders", "request_id": 2,
                    "account_id": "shared", "operator": "test",
                    "reason": "operator pause test"})
                self.assertTrue(paused["accepted"], paused)
                public.send_depth(12, 12)
                time.sleep(.15)
                self.assertFalse([row for row in trading.snapshot()["received"]
                                  if row["kind"] == "submit"])
                resumed = control(socket_path, {
                    "op": "resume_trading", "request_id": 3,
                    "account_id": "shared", "operator": "test",
                    "reason": "operator resume test"})
                self.assertTrue(resumed["accepted"], resumed)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 4})["accounts"][0]["ready"])
                public.send_depth(13, 13)
                first = wait_until(lambda: next((row for row in
                    trading.snapshot()["received"] if row["kind"] == "submit"), None))
                cancelled = control(socket_path, {
                    "op": "cancel_all", "request_id": 5,
                    "account_id": "shared", "operator": "test",
                    "reason": "operator cancel test"})
                self.assertTrue(cancelled["accepted"], cancelled)
                self.assertGreaterEqual(cancelled["cancel_requests_enqueued"], 1)
                audit = control(socket_path, {
                    "op": "account_controls", "request_id": 50,
                    "account_id": "shared", "limit": 2})
                self.assertEqual(
                    [(row["action"], row["action_name"], row["operator"],
                      row["reason"])
                     for row in audit["records"]],
                    [(0, "pause_new_orders", "test", "operator pause test"),
                     (1, "resume_trading", "test", "operator resume test")])
                next_page = control(socket_path, {
                    "op": "account_controls", "request_id": 51,
                    "account_id": "shared", "cursor": audit["next_cursor"]})
                self.assertEqual(
                    [(row["action"], row["action_name"], row["reason"])
                     for row in next_page["records"]],
                    [(3, "cancel_all", "operator cancel test")])
                self.assertGreater(next_page["next_cursor"],
                                   audit["next_cursor"])
                wait_until(lambda: trading.snapshot()["orders"][first["client_id"]]
                           ["status"] == "CANCELED")
                account = control(socket_path, {
                    "op": "status", "request_id": 6})["accounts"][0]
                self.assertTrue(account["manual_paused"])
                self.assertFalse(account["ready"])
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 7})["accepted"])
                stdout, stderr = process.communicate(timeout=12)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                with sqlite3.connect(state / "order_journal.sqlite") as db:
                    self.assertGreaterEqual(db.execute(
                        "SELECT COUNT(*) FROM events WHERE kind=10").fetchone()[0], 3)
                process = subprocess.Popen(
                    [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                    cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True)
                wait_until(lambda: socket_path.exists() or process.poll() is not None)
                if process.poll() is not None:
                    stdout, stderr = process.communicate(timeout=3)
                    self.fail(f"restart failed: {stdout} {stderr}")
                wait_until(lambda: (account := control(socket_path, {
                    "op": "status", "request_id": 8})["accounts"][0])
                    ["manual_paused"] and account["private_connected"] and
                    not account["syncing"], timeout=10)
                audit = control(socket_path, {
                    "op": "account_controls", "request_id": 52,
                    "account_id": "shared"})
                self.assertEqual([row["action"] for row in audit["records"]],
                                 [0, 1, 3])
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 9})["accepted"])
                stdout, stderr = process.communicate(timeout=12)
                self.assertEqual(process.returncode, 0, stdout + stderr)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_private_reconnect_reconciles_before_trading(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-reconnect-") as directory, \
                MockBinance() as public, TradingThread(fill_delay=30) as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                public.send_depth(12, 12)
                first = wait_until(lambda: next((row for row in
                    trading.snapshot()["received"] if row["kind"] == "submit"), None))
                trading.disconnect_private()
                wait_until(lambda: trading.snapshot()["private_connections"] >= 2,
                           timeout=10)
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 2})["accounts"][0]["ready"],
                           timeout=10)
                self.assertEqual([row["client_id"] for row in
                                  trading.snapshot()["received"]
                                  if row["kind"] == "submit"],
                                 [first["client_id"]])
                self.assertTrue(control(socket_path, {
                    "op": "stop", "request_id": 3})["accepted"])
                stdout, stderr = process.communicate(timeout=12)
                self.assertEqual(process.returncode, 0, stdout + stderr)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_restart_restores_open_order_before_new_trading(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-restart-") as directory, \
                MockBinance() as public, TradingThread(fill_delay=30) as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            command = [str(BINARY), f"--config={config}",
                       f"--state_dir={state}"]
            process = subprocess.Popen(command, cwd=ROOT,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                trading.wait_ready()
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 1})["accounts"][0]["ready"])
                public.send_depth(12, 12)
                first = wait_until(lambda: next((row for row in
                    trading.snapshot()["received"] if row["kind"] == "submit"), None))
                wait_until(lambda: control(socket_path, {
                    "op": "status", "request_id": 2})["accounts"][0]["open_orders"])
                process.kill()
                process.communicate(timeout=3)
                process = subprocess.Popen(command, cwd=ROOT,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, text=True)
                last_status = {}
                def restored():
                    try:
                        last_status.update(control(socket_path, {
                            "op": "status", "request_id": 3})["accounts"][0])
                        return last_status["ready"]
                    except (OSError, ValueError):
                        return False
                try:
                    wait_until(restored, timeout=10)
                except AssertionError:
                    output = process.communicate(timeout=1) if process.poll() is not None else ("", "")
                    self.fail(f"restart did not reconcile: status={last_status}, "
                              f"exit={process.poll()}, output={output}")
                self.assertEqual([row["client_id"] for row in
                                  trading.snapshot()["received"]
                                  if row["kind"] == "submit"],
                                 [first["client_id"]])
                self.assertTrue(control(socket_path,
                                        {"op": "stop", "request_id": 4})
                                ["accepted"])
                stdout, stderr = process.communicate(timeout=12)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                self.assertEqual(trading.snapshot()["orders"][first["client_id"]]
                                 ["status"], "CANCELED")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)

    def test_signed_socket_chain_and_independent_ledger(self):
        with tempfile.TemporaryDirectory(prefix="hquant-trade-e2e-") as directory, \
                MockBinance() as public, TradingThread() as trading:
            root = Path(directory)
            state = root / "state"
            state.mkdir()
            config = root / "config.yaml"
            config.write_text(configuration(
                (public.http.server_port, public.ws_port), trading.ports))
            process = subprocess.Popen(
                [str(BINARY), f"--config={config}", f"--state_dir={state}"],
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True,
            )
            try:
                socket_path = state / "control.sock"
                wait_until(socket_path.exists)
                trading.wait_ready()
                def account_status():
                    return control(socket_path, {"op": "status", "request_id": 1})["accounts"][0]
                try:
                    wait_until(lambda: account_status()["ready"])
                except AssertionError as error:
                    self.fail(f"account did not become ready: {account_status()}: {error}")
                public.send_depth(12, 12)
                first = wait_until(lambda: (orders[0] if
                    (orders := [row for row in trading.snapshot()["received"]
                               if row["kind"] == "submit"]) else None))
                client_id = first["client_id"]
                wait_until(lambda: trading.snapshot()["trades"])
                updated = control(socket_path, {
                    "op": "set_strategy", "request_id": 3, "shard_id": 0,
                    "expected_version": 1,
                    "patch": {"bid_spread": "0.002"},
                })
                self.assertTrue(updated["ok"], updated)
                wait_until(lambda: any(
                    row["kind"] == "cancel" and row["client_id"] == client_id
                    for row in trading.snapshot()["received"]))
                wait_until(lambda: trading.snapshot()["orders"][client_id]
                           ["status"] == "CANCELED")
                self.assertTrue(control(socket_path,
                                        {"op": "stop", "request_id": 4})
                                ["accepted"])
                stdout, stderr = process.communicate(timeout=12)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                venue = trading.snapshot()
                self.assertFalse([order for order in venue["orders"].values()
                                  if order["status"] in
                                  ("NEW", "PARTIALLY_FILLED")], venue["orders"])
                with sqlite3.connect(state / "history.sqlite") as db:
                    db.row_factory = sqlite3.Row
                    rows = [dict(row) for row in db.execute(
                        "SELECT * FROM account_events ORDER BY account_sequence")]
                    gaps = db.execute("SELECT COUNT(*) FROM history_gaps").fetchone()[0]
                self.assertEqual(gaps, 0)
                self.assertEqual([row["account_sequence"] for row in rows],
                                 list(range(1, len(rows) + 1)))
                self.assertEqual({row["client_order_id"] for row in rows
                                  if row["kind"] == 1},
                                 {row["client_id"] for row in venue["received"]
                                  if row["kind"] == "submit" and
                                  row["client_id"] in venue["orders"]})
                self.assertEqual(Counter(int(row["trade_id"]) for row in rows
                                         if row["kind"] == 6),
                                 Counter(trade["trade_id"] for trade in
                                         venue["trades"]))
                self.assertTrue(any(row["kind"] == 3 and
                                    row["client_order_id"] == client_id
                                    for row in rows))
                self.assertTrue(any(row["kind"] == 5 and
                                    row["client_order_id"] == client_id
                                    for row in rows))
                for asset, balance in venue["wallet"].items():
                    history = [row for row in rows if row["kind"] == 7 and
                               row["asset"] == asset]
                    self.assertTrue(history, asset)
                    self.assertEqual(int(history[-1]["available_nanos"]),
                                     balance["free"])
                    self.assertEqual(int(history[-1]["locked_nanos"]),
                                     balance["locked"])
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate(timeout=3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=BINARY)
    arguments, unittest_args = parser.parse_known_args()
    BINARY = arguments.binary.resolve()
    unittest.main(argv=[__file__, *unittest_args])
