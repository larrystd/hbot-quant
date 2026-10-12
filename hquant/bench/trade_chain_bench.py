#!/usr/bin/env python3
"""Run the existing market fixture through a real-socket simulated trade chain."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import time

from market.market_wire import (BINARY, MockWire, analyze_acks, calibrate_clock,
                                check_status, control, prepare, send_native,
                                spread_schedule)
from venue.mock_binance_server import MockBinanceTrading
from validate_trade_chain import validate


ROOT = Path(__file__).resolve().parents[2]


def trading_config(case_dir, symbols, public_ports, trading_ports,
                   refresh_ms=None):
    fixture = case_dir / "market_shards.yaml"
    if not fixture.exists():
        fixture = case_dir / "config.yaml"  # Earlier fixture layout.
    original = fixture.read_text()
    blocks = original.split("shards:\n", 1)[1].split("  - id: ")[1:]
    if len(blocks) < symbols:
        raise ValueError("fixture has fewer shards than requested")
    rest_port, ws_port = public_ports
    trade_rest_port, private_ws_port = trading_ports
    selected = blocks[:symbols]
    bases = [block.split("base_asset: ", 1)[1].split("\n", 1)[0]
             .replace("_", "") for block in selected]
    quote = selected[0].split("quote_asset: ", 1)[1].split("\n", 1)[0]
    rule_fields = ("price_increment", "base_increment",
                   "min_base_amount", "min_order_value")
    rules = {}
    for block in selected:
        symbol = block.split("symbol: ", 1)[1].split("\n", 1)[0]
        symbol = symbol.replace("_", "")
        rules[symbol] = {
            field: block.split(f"{field}: ", 1)[1].split("\n", 1)[0].strip('"')
            for field in rule_fields}
    lines = [
        "schema_version: 2", "input:", "  source: binance_public",
        "  rest_host: 127.0.0.1", f"  rest_port: {rest_port}",
        "  websocket_host: 127.0.0.1", f"  websocket_port: {ws_port}",
        "  tls: false", "accounts:", "  - id: shared",
        "    mode: pressure_sim", "    venue: binance_spot",
        "    trading_rest:", "      host: 127.0.0.1",
        f"      port: {trade_rest_port}", "      tls: false",
        "    private_ws:", "      host: 127.0.0.1",
        f"      port: {private_ws_port}", "      tls: false",
        "    initial_balances:",
    ]
    lines.extend(f'      {base}: "1000"' for base in bases)
    lines += [f'      {quote}: "1000000000"', "    hard_limits:"]
    lines.extend(f'      {base}: "1000"' for base in bases)
    lines += [f'      {quote}: "1000000000"',
              '    maker_fee_rate: "0.001"', "shards:"]
    for block in selected:
        fixture_name = block.split("symbol: ", 1)[1].split("\n", 1)[0]
        body = block.split("    executor:", 1)[0]
        body = body.replace(fixture_name, fixture_name.replace("_", ""))
        if refresh_ms is not None:
            body = body.replace("refresh_ms: 15000",
                                f"refresh_ms: {refresh_ms}")
            body = body.replace("refresh_ms: 60000",
                                f"refresh_ms: {refresh_ms}")
        lines.append("  - id: " + body.rstrip())
        lines.append("    account_id: shared")
    return "\n".join(lines) + "\n", quote, bases, rules


async def run(args):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < 8192:
        resource.setrlimit(resource.RLIMIT_NOFILE, (8192, hard))
    manifest = json.loads((args.case_dir / "manifest.json").read_text())
    duration_us = (args.duration_seconds * 1_000_000
                   if args.duration_seconds else manifest["duration_us"])
    data = prepare(args.case_dir, args.symbols, duration_us)
    spread_schedule(data, duration_us, args.spread_symbols_us)
    state = args.state_dir or Path(tempfile.mkdtemp(prefix="hquant-trade-chain-"))
    state.mkdir(parents=True, exist_ok=True)
    if (state / "history.sqlite").exists():
        raise ValueError("state directory already has a history database")
    data["state"] = state
    public = MockWire(data)
    public_ports = await public.start()
    quote = "CNY" if manifest["case"] == "sse" else "USD"
    balances = {symbol: "1000" for symbol in data["symbols"]}
    balances[quote] = "1000000000"
    trading = MockBinanceTrading(
        {symbol: (symbol, quote) for symbol in data["symbols"]}, balances)
    trading_ports = await trading.start()
    text, _, _, rules = trading_config(args.case_dir, args.symbols,
                                      public_ports, trading_ports,
                                      args.strategy_refresh_ms)
    trading.rules = rules
    config = state / "config.yaml"
    config.write_text(text)
    result = {"case": manifest["case"], "symbols": args.symbols,
              "duration_seconds": duration_us / 1e6,
              "state_dir": str(state), "valid": False}
    receiver = None
    process = None
    stdout = (state / "server.stdout").open("w")
    stderr = (state / "server.stderr").open("w")
    try:
        receiver_binary = state / "ack_receiver"
        subprocess.run(["clang++", "-O2", "-std=c++20",
                        str(ROOT / "hquant/bench/timing/ack_receiver.cc"),
                        "-o", str(receiver_binary)], check=True,
                       capture_output=True)
        receiver = subprocess.Popen(
            [str(receiver_binary), str(state / "acks.bin"),
             str(state / "ack_receiver.json")],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=(state / "ack_receiver.stderr").open("w"))
        ack_port = int(receiver.stdout.readline())
        process = subprocess.Popen(
            [str(BINARY), f"--config={config}", f"--state_dir={state}",
             f"--bench_output={state / 'probe.json'}",
             f"--bench_ack_port={ack_port}"],
            cwd=ROOT, stdout=stdout, stderr=stderr)
        started = time.monotonic()
        while not public.ready.is_set() or not trading.ws_ready.is_set():
            if process.poll() is not None:
                raise RuntimeError("quantserver exited during startup")
            if time.monotonic() - started > 120:
                raise TimeoutError("public or private WS startup timed out")
            await asyncio.sleep(.1)
        result["startup_seconds"] = time.monotonic() - started
        result["clock"] = await asyncio.to_thread(
            calibrate_clock, state / "control.sock")
        receiver.stdin.write(f"{len(data['events'])}\n".encode())
        receiver.stdin.flush()
        result["sender"], sent_ns, kinds = await send_native(
            data, public, args.speed, state, data["events"])
        receiver_exit = await asyncio.wait_for(
            asyncio.to_thread(receiver.wait), timeout=60)
        if receiver_exit != 0:
            raise RuntimeError("completion ACK receiver failed")
        result["completion_receiver"] = json.loads(
            (state / "ack_receiver.json").read_text())
        result["completion"] = analyze_acks(state / "acks.bin",
                                            sent_ns, kinds)
        deadline = time.monotonic() + 60
        status = None
        while time.monotonic() < deadline:
            status = await asyncio.to_thread(control, state / "control.sock",
                                             "status")
            if not check_status(data, status):
                break
            await asyncio.sleep(.1)
        if status is None:
            raise RuntimeError("no final control status")
        result["status_errors"] = check_status(data, status)
        result["account_before_stop"] = [
            {key: account[key] for key in
             ("id", "ready", "queued", "open_orders", "unknown_orders", "error")}
            for account in status["accounts"]]
        await asyncio.to_thread(control, state / "control.sock", "stop")
        result["server_exit"] = await asyncio.wait_for(
            asyncio.to_thread(process.wait), timeout=90)
        result["probe"] = json.loads((state / "probe.json").read_text())
        venue = trading.snapshot()
        (state / "venue_snapshot.json").write_text(json.dumps(venue) + "\n")
        result["trade_chain"] = validate(state / "history.sqlite", venue,
                                         sent_ns, duration_us / 1e6,
                                         result["probe"]["final_accounts"])
        result["public_wire"] = {
            "ws_connects": sum(public.ws_connects.values()),
            "snapshot_requests": sum(public.rest_requests.values()),
            "errors": public.errors[:10],
        }
        result["valid"] = (
            result["server_exit"] == 0 and not result["status_errors"] and
            not result["trade_chain"]["errors"] and
            not result["public_wire"]["errors"] and
            result["completion"]["received"] == len(data["events"]) and
            not result["completion"]["errors"] and
            result["public_wire"]["ws_connects"] == args.symbols and
            result["public_wire"]["snapshot_requests"] == args.symbols and
            result["trade_chain"]["submits"] > 0 and
            result["trade_chain"]["cancels"] > 0 and
            result["trade_chain"]["fills"] > 0)
        if args.symbols == 1000 and duration_us == 30_000_000:
            coverage = result["trade_chain"]["script_coverage"]
            result["valid"] = result["valid"] and all(
                coverage.get(kind, 0) > 0 for kind in
                ("NEW", "REJECTED", "PARTIALLY_FILLED", "FILLED",
                 "CANCELED", "cancel_fill_races", "duplicate_reports"))
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(process.wait), 10)
            except asyncio.TimeoutError:
                process.kill()
                await asyncio.to_thread(process.wait)
        if receiver and receiver.poll() is None:
            receiver.terminate()
            await asyncio.to_thread(receiver.wait)
        if receiver:
            receiver.stdin.close()
            receiver.stdout.close()
        await public.close()
        await trading.close()
        stdout.close()
        stderr.close()
        (state / "trade_chain_result.json").write_text(
            json.dumps(result, indent=2) + "\n")
    print(json.dumps({
        "valid": result["valid"], "case": result["case"],
        "symbols": result["symbols"],
        "duration_seconds": result["duration_seconds"],
        "startup_seconds": result.get("startup_seconds"),
        "completion": result.get("completion", {}).get("received"),
        "trade_chain": {key: value for key, value in
                        result.get("trade_chain", {}).items()
                        if key not in ("market_to_order",
                                       "private_report_to_apply",
                                       "market_to_private_report",
                                       "market_to_fill")},
        "status_errors": result.get("status_errors"),
        "state_dir": str(state),
    }, indent=2))
    if not result["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("case_dir", type=Path)
    parser.add_argument("--symbols", type=int, default=1000)
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--speed", type=float, default=1.0)
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--spread-symbols-us", type=int, default=0)
    parser.add_argument("--strategy-refresh-ms", type=int)
    options = parser.parse_args()
    if options.speed <= 0:
        parser.error("--speed must be positive")
    asyncio.run(run(options))
