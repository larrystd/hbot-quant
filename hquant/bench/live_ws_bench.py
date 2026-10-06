#!/usr/bin/env python3
"""Paced black-box market benchmark through hquant_server's live REST/WS input."""

import argparse
import asyncio
from array import array
import base64
from collections import Counter, defaultdict
import ctypes
from dataclasses import dataclass
import gc
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import resource
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
from urllib.parse import parse_qs, urlsplit


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "bazel-bin/hquant/src/hquant_server"
SCALE = 1_000_000_000
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
FNV_OFFSET = 14695981039346656037
FNV_PRIME = 1099511628211


def steady_ns():
    # On macOS Python's monotonic_ns() can have a process-local epoch. This
    # clock_gettime value is shared by the sender, ACK receiver, and the
    # benchmark-only C++ clock.
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


@dataclass
class WireEvent:
    at_us: int
    shard: int
    prefix: bytes  # JSON object without its final brace; timestamp added at send
    is_depth: bool


def distribution(samples, divisor=1000):
    if not samples:
        return {"count": 0}
    samples.sort()
    n = len(samples)
    return {"count": n, "mean_us": sum(samples) / n / divisor,
            "p50_us": samples[int(.50 * (n - 1))] / divisor,
            "p95_us": samples[int(.95 * (n - 1))] / divisor,
            "p99_us": samples[int(.99 * (n - 1))] / divisor,
            "p999_us": samples[int(.999 * (n - 1))] / divisor,
            "max_us": samples[-1] / divisor}


def price(ticks):
    return f"{ticks // 100}.{ticks % 100:02d}"


def hash_word(state, value):
    for _ in range(8):
        state = ((state ^ (value & 255)) * FNV_PRIME) & ((1 << 64) - 1)
        value >>= 8
    return state


def hash_words(state, words):
    for value in words:
        state = hash_word(state, value)
    return state


def wire_levels(current, previous):
    """Absolute Binance updates plus tombstones preserve full snapshot semantics."""
    old_prices = {p for p, _ in previous}
    new_prices = {p for p, _ in current}
    updates = [[price(p), str(q)] for p, q in current]
    updates.extend([price(p), "0"] for p in sorted(old_prices - new_prices))
    return updates


def iter_fixture(path):
    source = path.read_text()
    header = '"inputs":['
    offset = source.find(header)
    if offset < 0:
        raise ValueError("fixture missing inputs")
    offset += len(header)
    decoder = json.JSONDecoder()
    while source[offset] != ']':
        event, offset = decoder.raw_decode(source, offset)
        yield event
        if source[offset] == ',':
            offset += 1
        elif source[offset] != ']':
            raise ValueError("invalid fixture event separator")


def prepare(case_dir, symbols, duration_us):
    manifest = json.loads((case_dir / "manifest.json").read_text())
    if not 1 <= symbols <= manifest["stocks"]:
        raise ValueError("invalid symbol count")
    if not 0 < duration_us <= manifest["duration_us"]:
        raise ValueError("invalid duration")
    fixture_names = [f"{manifest['case'].upper()}_SIM_{i + 1:04d}"
                     for i in range(symbols)]
    # The replay fixtures use underscores, while the real Binance feed path
    # accepts only uppercase letters and digits in configured symbols.
    symbol_names = [name.replace("_", "") for name in fixture_names]
    events = []
    prior = [([], []) for _ in range(symbols)]
    final_books = [None] * symbols
    first_books = [None] * symbols
    counts = [Counter() for _ in range(symbols)]
    sequences = [9] * symbols
    trade_ids = [0] * symbols
    depth_digests = [FNV_OFFSET] * symbols
    trade_digests = [FNV_OFFSET] * symbols
    subscribed = [False] * symbols
    previous_at = -1
    for row in iter_fixture(case_dir / "replay.json"):
        shard, at_us = row["shard"], row["at_us"]
        if at_us >= duration_us:
            break
        if shard >= symbols:
            continue
        if at_us < previous_at:
            raise ValueError("fixture times decreased")
        previous_at = at_us
        if row.get("market") != fixture_names[shard]:
            raise ValueError("fixture symbol mismatch")
        kind = row["kind"]
        if kind == "subscribe":
            if subscribed[shard]:
                raise ValueError("duplicate fixture subscribe")
            subscribed[shard] = True
            continue
        if not subscribed[shard]:
            raise ValueError("fixture event before subscribe")
        if kind == "snapshot":
            bids, asks = row["bids"], row["asks"]
            if len(bids) != 10 or len(asks) != 10:
                raise ValueError("expected 10 levels on each side")
            if row["last_sequence"] != sequences[shard] + 1:
                raise ValueError("fixture depth sequence gap")
            b, a = wire_levels(bids, prior[shard][0]), wire_levels(asks, prior[shard][1])
            payload = {"e": "depthUpdate", "s": symbol_names[shard],
                       "U": row["last_sequence"], "u": row["last_sequence"],
                       "b": b, "a": a}
            words = [row["last_sequence"], row["last_sequence"], len(b)]
            for p, q in b:
                words.extend((int(p.replace(".", "")), int(q)))
            words.append(len(a))
            for p, q in a:
                words.extend((int(p.replace(".", "")), int(q)))
            depth_digests[shard] = hash_words(depth_digests[shard], words)
            prior[shard] = (bids, asks)
            final_books[shard] = (bids, asks)
            if first_books[shard] is None:
                first_books[shard] = (bids, asks)
            sequences[shard] += 1
            counts[shard]["depth"] += 1
        elif kind == "public_trade":
            if row["trade_id"] != trade_ids[shard] + 1:
                raise ValueError("fixture trade ID gap")
            payload = {"e": "trade", "s": symbol_names[shard],
                       "t": row["trade_id"], "p": price(row["price_ticks"]),
                       "q": str(row["quantity_lots"]),
                       "m": row["side"] == "sell"}
            trade_digests[shard] = hash_words(
                trade_digests[shard],
                (row["trade_id"], row["price_ticks"],
                 row["quantity_lots"], 1 if row["side"] == "buy" else 2))
            trade_ids[shard] += 1
            counts[shard]["trade"] += 1
        else:
            raise ValueError(f"unsupported live input kind {kind}")
        encoded = json.dumps(payload, separators=(",", ":")).encode()
        events.append(WireEvent(at_us, shard, encoded[:-1], kind == "snapshot"))
    if not all(subscribed) or not all(first_books):
        raise ValueError("fixture has symbols without initial depth")
    if duration_us == manifest["duration_us"] and symbols == manifest["stocks"]:
        assert sum(c["depth"] for c in counts) == manifest[
            "book_messages_including_initial_snapshots"]
        assert sum(c["trade"] for c in counts) == manifest["public_trades"]
        assert sequences == manifest["final_sequences"]
        assert trade_ids == manifest["final_trade_ids"]
    snapshots = {}
    for symbol, (bids, asks) in zip(symbol_names, first_books):
        body = {"lastUpdateId": 9,
                "bids": [[price(p), str(q)] for p, q in bids],
                "asks": [[price(p), str(q)] for p, q in asks]}
        snapshots[symbol] = json.dumps(body, separators=(",", ":")).encode()
    return {"manifest": manifest, "symbols": symbol_names, "events": events,
            "snapshots": snapshots, "counts": counts, "final_books": final_books,
            "sequences": sequences, "trade_ids": trade_ids,
            "depth_digests": depth_digests, "trade_digests": trade_digests}


def spread_schedule(data, duration_us, spread_us):
    """Spread synchronized symbols while preserving each symbol's event order."""
    if spread_us == 0:
        return
    symbols = len(data["symbols"])
    for event in data["events"]:
        phase = spread_us * event.shard // symbols
        event.at_us = ((event.at_us + phase) * duration_us //
                       (duration_us + spread_us))
    data["events"].sort(key=lambda event: (event.at_us, event.shard))


def live_config(case_dir, symbols, rest_port, ws_port, refresh_ms=None):
    original = (case_dir / "config.yaml").read_text()
    blocks = original.split("shards:\n", 1)[1].split("  - id: ")[1:]
    if len(blocks) < symbols:
        raise ValueError("not enough shard configurations")
    text = ("schema_version: 1\ninput:\n  source: binance_public\n"
            "  rest_host: 127.0.0.1\n  websocket_host: 127.0.0.1\n"
            f"  rest_port: {rest_port}\n  websocket_port: {ws_port}\n"
            "  tls: false\nshards:\n")
    for block in blocks[:symbols]:
        fixture_name = block.split("symbol: ", 1)[1].split("\n", 1)[0]
        block = block.replace(fixture_name, fixture_name.replace("_", ""))
        if refresh_ms is not None:
            block = block.replace("refresh_ms: 15000",
                                  f"refresh_ms: {refresh_ms}")
        text += "  - id: " + block
    return text


def frame(payload):
    size = len(payload)
    if size < 126:
        return bytes((0x81, size)) + payload
    if size <= 65535:
        return bytes((0x81, 126)) + size.to_bytes(2, "big") + payload
    return bytes((0x81, 127)) + size.to_bytes(8, "big") + payload


def control(path, op):
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(30)
        client.connect(str(path))
        client.sendall(json.dumps({"request_id": 1, "op": op}).encode() + b"\n")
        with client.makefile("rb") as stream:
            result = json.loads(stream.readline())
    if not result.get("ok"):
        raise RuntimeError(f"control {op}: {result}")
    return result


class MockWire:
    def __init__(self, data):
        self.data = data
        self.clients = {}
        self.ws_connects = Counter()
        self.ws_disconnects = Counter()
        self.rest_requests = Counter()
        self.errors = []
        self.ready = asyncio.Event()
        self.rest_server = None
        self.ws_server = None

    async def start(self):
        self.rest_server = await asyncio.start_server(
            self.handle_rest, "127.0.0.1", 0, backlog=2048)
        self.ws_server = await asyncio.start_server(
            self.handle_ws, "127.0.0.1", 0, backlog=2048)
        return (self.rest_server.sockets[0].getsockname()[1],
                self.ws_server.sockets[0].getsockname()[1])

    async def handle_rest(self, reader, writer):
        try:
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            method, target, _ = header.split(b"\r\n", 1)[0].decode().split(" ")
            query = urlsplit(target)
            values = parse_qs(query.query)
            symbol = values.get("symbol", [""])[0]
            if (method != "GET" or query.path != "/api/v3/depth" or
                    values.get("limit") != ["1000"] or
                    symbol not in self.data["snapshots"]):
                self.errors.append(f"invalid REST target {target}")
                body, code = b"{}", b"404 Not Found"
            else:
                self.rest_requests[symbol] += 1
                body, code = self.data["snapshots"][symbol], b"200 OK"
            writer.write(b"HTTP/1.1 " + code + b"\r\nContent-Type: application/json\r\n"
                         + b"Content-Length: " + str(len(body)).encode()
                         + b"\r\nConnection: close\r\n\r\n" + body)
            await writer.drain()
        except Exception as error:
            self.errors.append(f"REST: {error}")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def handle_ws(self, reader, writer):
        symbol = ""
        try:
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            first, *lines = request.decode().split("\r\n")
            method, target, _ = first.split(" ")
            query = urlsplit(target)
            streams = parse_qs(query.query).get("streams", [""])[0].split("/")
            if method != "GET" or query.path != "/stream" or len(streams) != 2:
                raise ValueError(f"invalid WS target {target}")
            name = streams[0].removesuffix("@depth")
            symbol = name.upper()
            if (streams != [name + "@depth", name + "@trade"] or
                    symbol not in self.data["snapshots"]):
                raise ValueError(f"invalid WS streams {target}")
            headers = {}
            for line in lines:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.lower()] = value.strip()
            key = headers["sec-websocket-key"]
            accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest())
            writer.write(b"HTTP/1.1 101 Switching Protocols\r\n"
                         b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
                         b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n")
            await writer.drain()
            self.ws_connects[symbol] += 1
            self.clients[symbol] = writer
            if len(self.clients) == len(self.data["symbols"]):
                self.ready.set()
            while await reader.read(4096):
                pass
        except Exception as error:
            self.errors.append(f"WS {symbol}: {error}")
        finally:
            if symbol and self.clients.get(symbol) is writer:
                del self.clients[symbol]
                self.ws_disconnects[symbol] += 1
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def close(self):
        for server in (self.rest_server, self.ws_server):
            if server:
                server.close()
                await server.wait_closed()
        for writer in list(self.clients.values()):
            writer.close()
        await asyncio.gather(*(writer.wait_closed() for writer in
                               list(self.clients.values())), return_exceptions=True)


def calibrate_clock(socket_path):
    measurements = []
    for _ in range(11):
        before = steady_ns()
        reply = control(socket_path, "clock")
        after = steady_ns()
        server = int(reply["steady_ns"])
        bench = int(reply["bench_ns"])
        measurements.append((after - before, server - (before + after) // 2,
                             bench - (before + after) // 2))
    rtt_ns, offset_ns, bench_offset_ns = min(measurements)
    return {"offset_ns": offset_ns, "bench_offset_ns": bench_offset_ns,
            "best_rtt_us": rtt_ns / 1000}


def receive_acks(pipe, stop, output):
    """Separate process timestamps completions without stalling the WS sender."""
    class MachTimebase(ctypes.Structure):
        _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

    timebase = MachTimebase()
    if ctypes.CDLL("/usr/lib/libSystem.B.dylib").mach_timebase_info(
            ctypes.byref(timebase)) != 0:
        raise RuntimeError("cannot read mach timebase")
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
        udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        # Darwin SO_TIMESTAMP_MONOTONIC / SCM_TIMESTAMP_MONOTONIC. The kernel
        # reports mach_absolute_time ticks at datagram arrival.
        udp.setsockopt(socket.SOL_SOCKET, 0x0800, 1)
        udp.bind(("127.0.0.1", 0))
        udp.settimeout(.2)
        pipe.send(udp.getsockname()[1])
        expected = pipe.recv()
        started_cpu = time.process_time_ns()
        records = bytearray()
        malformed = 0
        kernel_timestamped = 0
        while len(records) // 112 < expected and not stop.is_set():
            try:
                packet, ancillary, _, _ = udp.recvmsg(104, 256)
                received_ns = steady_ns()
                received_uptime_ns = time.clock_gettime_ns(time.CLOCK_UPTIME_RAW)
            except socket.timeout:
                continue
            if len(packet) != 96:
                malformed += 1
                continue
            fields = struct.unpack("!" + "Q" * 12, packet)
            kernel_ticks = next((
                struct.unpack("=Q", value)[0]
                for level, kind, value in ancillary
                if level == socket.SOL_SOCKET and kind == 0x04 and len(value) == 8
            ), None)
            if kernel_ticks is None:
                malformed += 1
                continue
            kernel_timestamped += 1
            kernel_uptime_ns = (kernel_ticks * timebase.numer //
                                timebase.denom)
            dispatch_ns = received_uptime_ns - kernel_uptime_ns
            send_to_kernel_ns = kernel_uptime_ns - fields[6]
            if (dispatch_ns < 0 or dispatch_ns > 60_000_000_000 or
                    send_to_kernel_ns < 0 or
                    send_to_kernel_ns > 60_000_000_000):
                malformed += 1
                continue
            records.extend(struct.pack("=" + "Q" * 14,
                                       *fields[:6], received_ns,
                                       send_to_kernel_ns, dispatch_ns,
                                       *fields[7:]))
    output.write_bytes(records)
    pipe.send({"received": len(records) // 112, "malformed": malformed,
               "kernel_timestamped": kernel_timestamped,
               "receiver_cpu_seconds":
               (time.process_time_ns() - started_cpu) / 1e9})
    pipe.close()


def analyze_acks(path, sent_ns, kinds):
    raw = path.read_bytes()
    expected = len(sent_ns) - 1
    errors = []
    if len(raw) % 112:
        errors.append(f"partial acknowledgement record: {len(raw)} bytes")
    write_path = path.parent / "ws_sender.write_ns.bin"
    native_write_ns = array("Q")
    if write_path.exists():
        with write_path.open("rb") as source:
            native_write_ns.fromfile(source, expected)
    seen = bytearray(len(sent_ns))
    samples = {name: [] for name in (
        "end_to_end", "sender_to_ws_frame_complete",
        "ws_frame_complete_to_feed", "sender_to_server_read",
        "server_receive_to_done",
        "server_parse", "server_handler", "ack_prepare",
        "ack_send_to_receive", "ack_send_to_kernel",
        "ack_receiver_dispatch", "ack_split_clock_residual",
        "outside_server", "sender_to_first_socket_read",
        "first_to_last_socket_read", "last_socket_read_to_frame_complete",
        "send_return_to_first_socket_read",
        "send_return_to_kevent_read_ready",
        "kevent_read_ready_to_first_socket_read")}
    by_kind = {
        "depth": {name: [] for name in samples},
        "trade": {name: [] for name in samples},
    }
    by_second = defaultdict(lambda: {"sender_to_server_read": [],
                                     "ack_send_to_receive": [],
                                     "end_to_end": []})
    first_sent = first_read = None
    last_received = last_done = 0
    read_times, done_times, received_times = [], [], []
    duplicates = 0
    clock_pair_gaps_over_100us = 0
    socket_reads_missing = socket_reads_multiple = 0
    read_armed_after_send = first_read_before_send_return = 0
    kevent_stamps_missing = kevent_stamps_stale = 0
    kevent_ready_before_send_return = 0
    for (sequence, frame_complete_ns, read_ns, parsed_ns, done_ns,
         ack_send_ns, received_ns, send_to_kernel_ns,
         receiver_dispatch_ns, read_armed_ns, first_socket_read_ns,
         last_socket_read_ns, socket_reads, kevent_read_ready_ns) in struct.iter_unpack(
            "=" + "Q" * 14, raw[:len(raw) // 112 * 112]):
        if sequence < 1 or sequence > expected:
            errors.append(f"ack ID outside sent range: {sequence}")
            continue
        if seen[sequence]:
            duplicates += 1
            continue
        seen[sequence] = 1
        if not (sent_ns[sequence] <= frame_complete_ns <= read_ns <=
                parsed_ns <= done_ns <=
                ack_send_ns <= received_ns):
            errors.append(f"ack timing order invalid at ID {sequence}")
            continue
        if socket_reads and not (read_armed_ns <= first_socket_read_ns <=
                                 last_socket_read_ns <= frame_complete_ns):
            errors.append(f"socket read timing invalid at ID {sequence}")
            continue
        elapsed = received_ns - sent_ns[sequence]
        ack_elapsed = received_ns - ack_send_ns
        split_residual_ns = abs(
            ack_elapsed - send_to_kernel_ns - receiver_dispatch_ns)
        if split_residual_ns > 100_000:
            # The two server clock calls can be preempted between calls. All
            # three interval measurements remain valid on their own clocks.
            clock_pair_gaps_over_100us += 1
        durations = {
            "end_to_end": elapsed,
            "sender_to_ws_frame_complete": frame_complete_ns - sent_ns[sequence],
            "ws_frame_complete_to_feed": read_ns - frame_complete_ns,
            "sender_to_server_read": read_ns - sent_ns[sequence],
            "server_receive_to_done": done_ns - read_ns,
            "server_parse": parsed_ns - read_ns,
            "server_handler": done_ns - parsed_ns,
            "ack_prepare": ack_send_ns - done_ns,
            "ack_send_to_receive": ack_elapsed,
            "ack_send_to_kernel": send_to_kernel_ns,
            "ack_receiver_dispatch": receiver_dispatch_ns,
            "ack_split_clock_residual": split_residual_ns,
            "outside_server": elapsed - (done_ns - read_ns),
        }
        if read_armed_ns > sent_ns[sequence]:
            read_armed_after_send += 1
        if not socket_reads:
            socket_reads_missing += 1
        else:
            socket_reads_multiple += socket_reads > 1
            durations["sender_to_first_socket_read"] = (
                first_socket_read_ns - sent_ns[sequence])
            durations["first_to_last_socket_read"] = (
                last_socket_read_ns - first_socket_read_ns)
            durations["last_socket_read_to_frame_complete"] = (
                frame_complete_ns - last_socket_read_ns)
            if native_write_ns:
                send_return_ns = sent_ns[sequence] + native_write_ns[sequence-1]
                first_read_before_send_return += first_socket_read_ns < send_return_ns
                durations["send_return_to_first_socket_read"] = (
                    first_socket_read_ns - send_return_ns)
                if not kevent_read_ready_ns:
                    kevent_stamps_missing += 1
                elif not (max(read_armed_ns, sent_ns[sequence]) <=
                          kevent_read_ready_ns <= first_socket_read_ns):
                    kevent_stamps_stale += 1
                else:
                    kevent_ready_before_send_return += (
                        kevent_read_ready_ns < send_return_ns)
                    durations["send_return_to_kevent_read_ready"] = (
                        kevent_read_ready_ns - send_return_ns)
                    durations["kevent_read_ready_to_first_socket_read"] = (
                        first_socket_read_ns - kevent_read_ready_ns)
        group = by_kind["depth" if kinds[sequence] else "trade"]
        for name, value in durations.items():
            samples[name].append(value)
            group[name].append(value)
        second = (sent_ns[sequence] - sent_ns[1]) // 1_000_000_000
        for name in by_second[second]:
            by_second[second][name].append(durations[name])
        first_sent = min(first_sent, sent_ns[sequence]) if first_sent else sent_ns[sequence]
        first_read = min(first_read, read_ns) if first_read else read_ns
        last_received = max(last_received, received_ns)
        last_done = max(last_done, done_ns)
        read_times.append(read_ns)
        done_times.append(done_ns)
        received_times.append(received_ns)
    missing = expected - sum(seen)
    if missing:
        errors.append(f"missing {missing} acknowledgement IDs")
    if duplicates:
        errors.append(f"duplicate {duplicates} acknowledgement IDs")
    e2e_seconds = (last_received - first_sent) / 1e9 if first_sent else 0
    server_seconds = (last_done - first_read) / 1e9 if first_read else 0
    def peak_pending(completions):
        completions.sort()
        finished = peak = 0
        for sent_count, sent_time in enumerate(sent_ns[1:], 1):
            while finished < len(completions) and completions[finished] <= sent_time:
                finished += 1
            peak = max(peak, sent_count - finished)
        return peak

    received_per_second = Counter(
        (stamp - first_sent) // 1_000_000_000 for stamp in received_times)
    done_per_second = Counter(
        (stamp - first_sent) // 1_000_000_000 for stamp in done_times)
    return {"expected": expected, "received": len(raw) // 112,
            "unique": sum(seen), "errors": errors[:20],
            "clock_pair_gaps_over_100us": clock_pair_gaps_over_100us,
            "socket_reads_missing": socket_reads_missing,
            "socket_reads_multiple": socket_reads_multiple,
            "read_armed_after_send": read_armed_after_send,
            "first_read_before_send_return": first_read_before_send_return,
            "kevent_stamps_missing": kevent_stamps_missing,
            "kevent_stamps_stale": kevent_stamps_stale,
            "kevent_ready_before_send_return": kevent_ready_before_send_return,
            "pending_before_read_peak": peak_pending(read_times),
            "pending_before_server_done_peak": peak_pending(done_times),
            "pending_before_ack_peak": peak_pending(received_times),
            "server_completed_per_second": dict(sorted(done_per_second.items())),
            "acknowledged_per_second": dict(sorted(received_per_second.items())),
            **{name: distribution(values) for name, values in samples.items()},
            "by_kind": {name: {key: distribution(values)
                               for key, values in groups.items()}
                        for name, groups in by_kind.items()},
            "by_second": {str(second): {key: distribution(values)
                                       for key, values in groups.items()}
                          for second, groups in by_second.items()},
            "end_to_end_qps": expected / e2e_seconds if e2e_seconds else 0,
            "server_completed_qps": expected / server_seconds if server_seconds else 0,
            "end_to_end_seconds": e2e_seconds,
            "server_window_seconds": server_seconds}


async def send_paced(data, wire, speed, clock_offset_ns, events=None,
                     completion_acks=False, prebuild_frames=False):
    events = data["events"] if events is None else events
    writers = [wire.clients[symbol] for symbol in data["symbols"]]
    prepared_packets = None
    preparation_seconds = preparation_cpu_seconds = 0
    if prebuild_frames:
        if not completion_acks:
            raise ValueError("prebuilt frames require sequence-tagged messages")
        prepare_start = steady_ns()
        prepare_cpu_start = time.process_time_ns()
        prepared_packets = []
        for sequence, event in enumerate(events, 1):
            payload = event.prefix + b',"bench_seq":' + str(sequence).encode()
            prepared_packets.append(frame(payload + b'}'))
            # The prepared WS frame replaces the JSON prefix after warmup.
            event.prefix = b''
        preparation_seconds = (steady_ns() - prepare_start) / 1e9
        preparation_cpu_seconds = (time.process_time_ns() - prepare_cpu_start) / 1e9
    scheduled_start = steady_ns() + 100_000_000
    begun_cpu = time.process_time_ns()
    lag = []
    lag_by_second = defaultdict(list)
    call_time = []
    peak_buffer = 0
    sent = 0
    sent_ns = array("Q", [0])
    kinds = array("B", [0])
    index = 0
    while index < len(events):
        target = scheduled_start + int(events[index].at_us * 1000 / speed)
        now = steady_ns()
        if now < target:
            await asyncio.sleep((target - now) / 1e9)
            continue
        # Deliver all due events before sleeping again. This never waits for
        # an application acknowledgement or for a receiver status call.
        while index < len(events):
            event = events[index]
            due = scheduled_start + int(event.at_us * 1000 / speed)
            before = steady_ns()
            if due > before:
                break
            writer = writers[event.shard]
            if writer.is_closing():
                raise RuntimeError(f"WS closed before event {index}")
            if prepared_packets is not None:
                packet = prepared_packets[index]
            elif completion_acks:
                payload = event.prefix + b',"bench_seq":' + str(index + 1).encode()
                packet = frame(payload + b'}')
            else:
                sender_clock = (before + clock_offset_ns) % (1 << 64)
                payload = (event.prefix + b',"bench_sent_ns":' +
                           str(sender_clock).encode() + b',"bench_at_us":' +
                           str(event.at_us).encode())
                packet = frame(payload + b'}')
            sent_at = steady_ns()
            writer.write(packet)
            after = steady_ns()
            if completion_acks:
                sent_ns.append(sent_at)
                kinds.append(event.is_depth)
            lag.append(sent_at - due)
            lag_by_second[event.at_us // 1_000_000].append(sent_at - due)
            call_time.append(after - sent_at)
            peak_buffer = max(peak_buffer, writer.transport.get_write_buffer_size())
            sent += 1
            index += 1
        if index % 1000 == 0:
            await asyncio.sleep(0)
    last_write_ns = steady_ns()
    await asyncio.gather(*(writer.drain() for writer in writers))
    drained_ns = steady_ns()
    worst_seconds = sorted(
        ({"second": second, **distribution(values)}
         for second, values in lag_by_second.items()),
        key=lambda entry: entry["p99_us"], reverse=True)[:10]
    metrics = {"sent": sent, "schedule_seconds":
            (events[-1].at_us / 1e6 / speed) if events else 0,
            "frame_prepare_seconds": preparation_seconds,
            "frame_prepare_cpu_seconds": preparation_cpu_seconds,
            "send_seconds": (last_write_ns - scheduled_start) / 1e9,
            "drain_seconds": (drained_ns - last_write_ns) / 1e9,
            "sender_cpu_seconds": (time.process_time_ns() - begun_cpu) / 1e9,
            "peak_transport_buffer_bytes": peak_buffer,
            "worst_schedule_seconds": worst_seconds,
            "schedule_lateness": distribution(lag),
            "write_call": distribution(call_time)}
    return metrics, sent_ns, kinds


async def send_native(data, wire, speed, state, events):
    """Send the same WS frames from inherited sockets outside Python's loop."""
    binary = state / "ws_sender"
    subprocess.run(
        ["clang++", "-O3", "-std=c++17",
         str(ROOT / "hquant/bench/ws_sender.cc"), "-o", str(binary)],
        check=True, capture_output=True)
    input_path = state / "ws_sender.events"
    prepare_start = steady_ns()
    prepare_cpu_start = time.process_time_ns()
    kinds = array("B", [0])
    with input_path.open("wb") as output:
        output.write(struct.pack("=Q", len(events)))
        for sequence, event in enumerate(events, 1):
            packet = frame(event.prefix + b',"bench_seq":' +
                           str(sequence).encode() + b'}')
            output.write(struct.pack("=QII", int(event.at_us * 1000 / speed),
                                     event.shard, len(packet)))
            output.write(packet)
            kinds.append(event.is_depth)
            event.prefix = b''
    preparation_seconds = (steady_ns() - prepare_start) / 1e9
    preparation_cpu_seconds = (time.process_time_ns() - prepare_cpu_start) / 1e9
    sockets = [wire.clients[symbol].get_extra_info("socket").fileno()
               for symbol in data["symbols"]]
    prefix = state / "ws_sender"
    with (state / "ws_sender.stderr").open("w") as errors:
        process = subprocess.Popen(
            [str(binary), str(input_path), str(prefix),
             *(str(fd) for fd in sockets)],
            pass_fds=tuple(sockets), stderr=errors)
        exit_code = await asyncio.to_thread(process.wait)
    if exit_code != 0:
        raise RuntimeError("native WS sender failed")
    native = json.loads((state / "ws_sender.json").read_text())
    sent_ns = array("Q")
    with (state / "ws_sender.sent_ns.bin").open("rb") as samples:
        sent_ns.fromfile(samples, len(events) + 1)
    lag_ns = array("Q")
    with (state / "ws_sender.lag_ns.bin").open("rb") as samples:
        lag_ns.fromfile(samples, len(events))
    write_ns = array("Q")
    with (state / "ws_sender.write_ns.bin").open("rb") as samples:
        write_ns.fromfile(samples, len(events))
    lag_by_second = defaultdict(list)
    for event, lag in zip(events, lag_ns):
        lag_by_second[event.at_us // 1_000_000].append(lag)
    worst_seconds = sorted(
        ({"second": second, **distribution(values)}
         for second, values in lag_by_second.items()),
        key=lambda entry: entry["p99_us"], reverse=True)[:10]
    metrics = {
        **native,
        "schedule_seconds": events[-1].at_us / 1e6 / speed,
        "frame_prepare_seconds": preparation_seconds,
        "frame_prepare_cpu_seconds": preparation_cpu_seconds,
        "drain_seconds": 0,
        "peak_transport_buffer_bytes": 0,
        "worst_schedule_seconds": worst_seconds,
        "schedule_lateness": distribution(list(lag_ns)),
        "write_call": distribution(list(write_ns)),
    }
    return metrics, sent_ns, kinds


async def prewarm_depth(data, wire, clock_offset_ns):
    warmups = []
    timed = []
    seen = set()
    for event in data["events"]:
        if event.prefix.startswith(b'{"e":"depthUpdate"') and event.shard not in seen:
            seen.add(event.shard)
            warmups.append(event)
        else:
            timed.append(event)
    if len(warmups) != len(data["symbols"]):
        raise AssertionError("warmup depth missing for a symbol")
    for event in warmups:
        now = steady_ns()
        sender_clock = (now + clock_offset_ns) % (1 << 64)
        payload = (event.prefix + b',"bench_sent_ns":' +
                   str(sender_clock).encode() + b',"bench_at_us":' +
                   str(event.at_us).encode() + b'}')
        wire.clients[data["symbols"][event.shard]].write(frame(payload))
    await asyncio.gather(*(writer.drain() for writer in wire.clients.values()))
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status = await asyncio.to_thread(control, data["state"] / "control.sock", "status")
        if (len(wire.rest_requests) == len(data["symbols"]) and
                all(shard["book_state"] == "live" and
                    shard["applied_depth_events"] == 1 for shard in status["shards"]) and
                status["history_health"]["queued"] == 0):
            return len(warmups), timed
        await asyncio.sleep(.05)
    raise TimeoutError("warmup depth did not reach Live on every symbol")


async def sample_process(pid, state, start_seconds, duration_seconds):
    await asyncio.sleep(start_seconds)
    output = state / "cpu.sample.txt"
    result = await asyncio.to_thread(
        subprocess.run,
        ["sample", str(pid), str(duration_seconds), "1", "-file", str(output)],
        capture_output=True, text=True, timeout=duration_seconds + 30)
    return {"exit": result.returncode, "output": str(output),
            "stderr": result.stderr[-500:]}


def check_status(data, response):
    shards = response["shards"]
    errors = []
    if len(shards) != len(data["symbols"]):
        errors.append(f"status shard count {len(shards)}")
        return errors
    for i, status in enumerate(shards):
        expected = data["counts"][i]
        checks = {
            "id": i,
            "book_state": "live",
            "connection_epoch": 1,
            "last_book_sequence": data["sequences"][i],
            "applied_depth_events": expected["depth"],
            "accepted_trades": expected["trade"],
            "last_trade_id": data["trade_ids"][i],
            "depth_digest": str(data["depth_digests"][i]),
            "trade_digest": str(data["trade_digests"][i]),
            "bids": data["final_books"][i][0],
            "asks": data["final_books"][i][1],
            "market_notices": expected["depth"] + expected["trade"],
            "last_error": "",
        }
        for key, value in checks.items():
            if status.get(key) != value:
                errors.append(f"shard {i} {key}: got {status.get(key)!r}, expected {value!r}")
                if len(errors) >= 20:
                    return errors
    health = response["history_health"]
    if health["dropped"] or health["error"]:
        errors.append(f"history health {health}")
    return errors


def validate_history(database, data):
    """Check durable sequence, order transitions, funds, fees, and holds."""
    quote = data["manifest"]["case"] == "sse" and "CNY" or "USD"
    base_increment = (100 if quote == "CNY" else 1) * SCALE
    fee_rate = SCALE // 1000
    checked = 0
    seen_shards = 0
    balances = Counter()
    with sqlite3.connect(database) as db:
        db.row_factory = sqlite3.Row
        if db.execute("SELECT COUNT(*) FROM history_gaps").fetchone()[0]:
            raise AssertionError("history_gaps is not empty")
        if db.execute("SELECT COUNT(DISTINCT run_id) FROM history_records").fetchone()[0] != 1:
            raise AssertionError("history contains multiple or no runs")
        cursor = db.execute("SELECT * FROM history_records ORDER BY shard, shard_sequence")
        current = -1
        active = {}
        submitted = {}
        fill_pending = {}
        totals = {}
        next_seq = 1
        seen_balances = set()

        def finish(shard):
            if shard < 0:
                return
            if active or fill_pending:
                raise AssertionError(f"shard {shard} has unfinished orders/fills")
            if seen_balances != {data["symbols"][shard], quote}:
                raise AssertionError(f"shard {shard} missing balance rows")

        for raw in cursor:
            row = dict(raw)
            shard = row["shard"]
            if shard != current:
                finish(current)
                if shard != current + 1:
                    raise AssertionError("history missing a shard")
                current, next_seq = shard, 1
                seen_shards += 1
                active, submitted, fill_pending = {}, {}, {}
                totals = {data["symbols"][shard]: 1000 * SCALE,
                          quote: 1_000_000 * SCALE}
                seen_balances = set()
            if row["shard_sequence"] != next_seq:
                raise AssertionError(f"shard {shard} history sequence gap")
            next_seq += 1
            checked += 1
            kind = row["kind"]
            oid = int(row["order_id"])
            quantity = int(row["quantity_nanos"])
            price_nanos = int(row["price_nanos"])
            remaining = int(row["remaining_nanos"])
            if kind == 1:  # submit command
                if row["dispatched"]:
                    if oid in submitted:
                        raise AssertionError("duplicate submit ID")
                    submitted[oid] = (row["buy"] == 1, price_nanos, quantity)
            elif kind == 3:  # accepted
                if oid not in submitted or oid in active:
                    raise AssertionError("accepted without submit")
                buy, expected_price, expected_quantity = submitted[oid]
                if (price_nanos, quantity, remaining) != (
                        expected_price, expected_quantity, expected_quantity):
                    raise AssertionError("accepted order fields mismatch")
                active[oid] = [buy, price_nanos, remaining]
            elif kind == 8:  # rejected
                if oid not in submitted or oid in active:
                    raise AssertionError("rejected order invalid")
            elif kind == 2:  # cancel command
                if row["dispatched"] and oid not in active:
                    raise AssertionError("cancel without active order")
            elif kind == 5:  # cancelled
                if oid not in active or remaining != active[oid][2]:
                    raise AssertionError("cancelled remaining mismatch")
                del active[oid]
            elif kind == 6:  # fill
                if oid not in active or quantity <= 0 or quantity > active[oid][2]:
                    raise AssertionError("invalid fill")
                buy = active[oid][0]
                gross = price_nanos * quantity // SCALE
                fee = (gross * fee_rate + SCALE - 1) // SCALE
                if int(row["fee_nanos"]) != fee:
                    raise AssertionError("fill fee mismatch")
                base = data["symbols"][shard]
                totals[base] += quantity if buy else -quantity
                totals[quote] += -gross - fee if buy else gross - fee
                fill_pending[oid] = quantity
            elif kind in (4, 9):  # filled or partially filled
                if oid not in active or oid not in fill_pending:
                    raise AssertionError("fill report missing fill")
                active[oid][2] -= fill_pending.pop(oid)
                if remaining != active[oid][2] or (kind == 4) != (remaining == 0):
                    raise AssertionError("fill report remaining mismatch")
                if kind == 4:
                    del active[oid]
            elif kind == 7:  # balance
                asset = row["asset"]
                if asset not in totals or int(row["total_nanos"]) != totals[asset]:
                    raise AssertionError("balance total mismatch")
                frozen = 0
                for buy, order_price, order_remaining in active.values():
                    if (asset == quote) != buy:
                        continue
                    if buy:
                        gross = (order_price * order_remaining + SCALE - 1) // SCALE
                        fee = (gross * fee_rate + SCALE - 1) // SCALE
                        frozen += gross + fee + order_remaining // base_increment
                    else:
                        frozen += order_remaining
                if int(row["available_nanos"]) != totals[asset] - frozen:
                    raise AssertionError(f"shard {shard} balance available mismatch")
                seen_balances.add(asset)
                balances[asset] += 1
            else:
                raise AssertionError(f"unknown history kind {kind}")
        finish(current)
        if seen_shards != len(data["symbols"]):
            raise AssertionError("history missing a shard")
    return {"records": checked, "balance_rows": sum(balances.values()),
            "shards_with_records": seen_shards}


async def run(args):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < 8192:
        resource.setrlimit(resource.RLIMIT_NOFILE, (8192, hard))
    duration_us = args.duration_seconds * 1_000_000 if args.duration_seconds else \
        json.loads((args.case_dir / "manifest.json").read_text())["duration_us"]
    data = prepare(args.case_dir, args.symbols, duration_us)
    spread_schedule(data, duration_us, args.spread_symbols_us)
    state = args.state_dir or Path(tempfile.mkdtemp(prefix="hquant-live-"))
    state.mkdir(parents=True, exist_ok=True)
    if (state / "history.sqlite").exists():
        raise ValueError("state directory already contains a history database")
    data["state"] = state
    wire = MockWire(data)
    rest_port, ws_port = await wire.start()
    configuration = state / "config.yaml"
    configuration.write_text(live_config(
        args.case_dir, args.symbols, rest_port, ws_port,
        args.strategy_refresh_ms))
    result = {"case": data["manifest"]["case"], "symbols": args.symbols,
              "strategy_refresh_ms": args.strategy_refresh_ms or 15000,
              "spread_symbols_us": args.spread_symbols_us,
              "scheduled_peak_1ms": max(Counter(
                  event.at_us // 1000 for event in data["events"]).values()),
              "measurement_mode": ("completion_acks" if args.completion_acks else
                                   "no_ack_control" if args.no_ack_control else
                                   "legacy"),
              "duration_seconds": duration_us / 1e6, "speed": args.speed,
              "state_dir": str(state), "expected_depth":
              sum(c["depth"] for c in data["counts"]), "expected_trades":
              sum(c["trade"] for c in data["counts"]),
              "kevent_probe": args.kevent_probe}
    stdout = (state / "server.stdout").open("w")
    stderr = (state / "server.stderr").open("w")
    process = None
    receiver = receiver_pipe = receiver_stop = None
    native_receiver = native_receiver_stderr = None
    ack_started = False
    try:
        server_env = None
        if args.kevent_probe:
            probe_library = state / "libkevent_probe.dylib"
            subprocess.run(
                ["clang++", "-O2", "-std=c++17", "-dynamiclib",
                 "-Wno-deprecated-declarations",
                 str(ROOT / "hquant/bench/kevent_probe_darwin.cc"),
                 "-o", str(probe_library)],
                check=True, capture_output=True)
            server_env = os.environ.copy()
            inserted = server_env.get("DYLD_INSERT_LIBRARIES", "")
            server_env["DYLD_INSERT_LIBRARIES"] = (
                str(probe_library) + (":" + inserted if inserted else ""))
        ack_port = 0
        if args.completion_acks:
            if args.native_ack_receiver:
                binary = state / "ack_receiver"
                subprocess.run(
                    ["clang++", "-O3", "-std=c++17",
                     str(ROOT / "hquant/bench/ack_receiver.cc"),
                     "-o", str(binary)], check=True, capture_output=True)
                native_receiver_stderr = (state / "ack_receiver.stderr").open("w")
                native_receiver = subprocess.Popen(
                    [str(binary), str(state / "acks.bin"),
                     str(state / "ack_receiver.json")],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=native_receiver_stderr)
                port_line = native_receiver.stdout.readline()
                if not port_line:
                    raise RuntimeError("native ACK receiver did not bind")
                ack_port = int(port_line)
            else:
                context = multiprocessing.get_context("spawn")
                receiver_pipe, child_pipe = context.Pipe()
                receiver_stop = context.Event()
                receiver = context.Process(
                    target=receive_acks,
                    args=(child_pipe, receiver_stop, state / "acks.bin"))
                receiver.start()
                child_pipe.close()
                if not await asyncio.to_thread(receiver_pipe.poll, 10):
                    raise RuntimeError("completion receiver did not bind")
                ack_port = receiver_pipe.recv()
        process = subprocess.Popen(
            [str(BINARY), f"--config={configuration}",
             f"--state_dir={state}", f"--bench_output={state / 'probe.json'}"] +
            ([f"--bench_ack_port={ack_port}"] if ack_port else []),
            cwd=ROOT, stdout=stdout, stderr=stderr, env=server_env)
        start_wait = time.monotonic()
        while not wire.ready.is_set():
            if process.poll() is not None:
                raise RuntimeError(f"server exited during startup: {stderr.name}")
            if time.monotonic() - start_wait > 90:
                raise TimeoutError(f"only {len(wire.clients)} WS connections in 90s")
            await asyncio.sleep(.1)
        result["startup_seconds"] = time.monotonic() - start_wait
        print(f"connected {len(wire.clients)} symbols in {result['startup_seconds']:.2f}s",
              file=sys.stderr, flush=True)
        result["clock"] = await asyncio.to_thread(calibrate_clock,
                                                   state / "control.sock")
        expected_events = len(data["events"])
        timed_events = None
        result["warmup_messages"] = 0
        if args.prewarm_first_depth:
            result["warmup_messages"], timed_events = await prewarm_depth(
                data, wire, result["clock"]["offset_ns"])
        if receiver_pipe:
            receiver_pipe.send(len(timed_events if timed_events is not None
                                   else data["events"]))
            ack_started = True
        if native_receiver:
            native_receiver.stdin.write(
                f"{len(timed_events if timed_events is not None else data['events'])}\n"
                .encode())
            native_receiver.stdin.flush()
            ack_started = True
        sample_task = None
        if args.sample_start_seconds is not None:
            sample_task = asyncio.create_task(sample_process(
                process.pid, state, args.sample_start_seconds,
                args.sample_duration_seconds))
        if args.native_ws_sender:
            result["sender"], sent_ns, kinds = await send_native(
                data, wire, args.speed, state,
                timed_events if timed_events is not None else data["events"])
        else:
            gc_was_enabled = gc.isenabled()
            if args.disable_gc_during_send:
                gc.disable()
            try:
                result["sender"], sent_ns, kinds = await send_paced(
                    data, wire, args.speed, result["clock"]["offset_ns"],
                    timed_events, args.completion_acks or args.no_ack_control,
                    args.prebuild_frames)
            finally:
                if gc_was_enabled:
                    gc.enable()
        if receiver:
            await asyncio.to_thread(receiver.join, 20)
            if receiver.is_alive():
                receiver_stop.set()
                await asyncio.to_thread(receiver.join, 5)
            if receiver.exitcode != 0 or not receiver_pipe.poll(1):
                raise RuntimeError("completion receiver exited without results")
            result["completion_receiver"] = receiver_pipe.recv()
            result["completion"] = analyze_acks(state / "acks.bin", sent_ns, kinds)
            with (state / "sent_ns.bin").open("wb") as sent_file:
                sent_ns.tofile(sent_file)
        if native_receiver:
            native_exit = await asyncio.wait_for(
                asyncio.to_thread(native_receiver.wait), timeout=20)
            if native_exit != 0:
                raise RuntimeError("native ACK receiver exited without results")
            result["completion_receiver"] = json.loads(
                (state / "ack_receiver.json").read_text())
            result["completion"] = analyze_acks(state / "acks.bin", sent_ns, kinds)
            with (state / "sent_ns.bin").open("wb") as sent_file:
                sent_ns.tofile(sent_file)
        if sample_task:
            result["sample"] = await sample_task
        result["clock_after"] = await asyncio.to_thread(
            calibrate_clock, state / "control.sock")
        result["clock_drift_us"] = (
            result["clock_after"]["offset_ns"] - result["clock"]["offset_ns"]
        ) / 1000
        result["bench_clock_drift_us"] = (
            result["clock_after"]["bench_offset_ns"] -
            result["clock"]["bench_offset_ns"]) / 1000
        print(f"sent {result['sender']['sent']} frames in "
              f"{result['sender']['send_seconds']:.2f}s; waiting for receiver",
              file=sys.stderr, flush=True)
        final_status = None
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            try:
                final_status = await asyncio.to_thread(control, state / "control.sock", "status")
            except (OSError, ValueError):
                await asyncio.sleep(.1)
                continue
            if not check_status(data, final_status):
                break
            await asyncio.sleep(.1)
        if final_status is None:
            raise RuntimeError("no control status after send")
        result["status_errors"] = check_status(data, final_status)
        result["status_totals"] = {
            "applied_depth": sum(s["applied_depth_events"] for s in final_status["shards"]),
            "accepted_trades": sum(s["accepted_trades"] for s in final_status["shards"]),
            "active_orders": sum(s["active_orders"] for s in final_status["shards"]),
            "strategy_timer_ticks": sum(s["strategy_timer_ticks"] for s in final_status["shards"]),
        }
        result["wire"] = {"ws_connects": sum(wire.ws_connects.values()),
                          "ws_disconnects_before_stop": sum(wire.ws_disconnects.values()),
                          "rest_requests": sum(wire.rest_requests.values()),
                          "errors": wire.errors[:10]}
        await asyncio.to_thread(control, state / "control.sock", "stop")
        result["server_exit"] = await asyncio.wait_for(
            asyncio.to_thread(process.wait), timeout=30)
        result["probe"] = json.loads((state / "probe.json").read_text())
        result["history"] = validate_history(state / "history.sqlite", data)
        result["valid"] = (not result["status_errors"] and
                           not result["wire"]["errors"] and
                           result["wire"]["ws_connects"] == args.symbols and
                           result["wire"]["ws_disconnects_before_stop"] == 0 and
                           result["wire"]["rest_requests"] == args.symbols and
                           result["sender"]["sent"] +
                           result["warmup_messages"] == expected_events and
                           result["probe"]["live_wire_messages"] == expected_events and
                           result["probe"]["bad_sender_timestamps"] == 0 and
                           (not args.completion_acks or (
                               not result["completion"]["errors"] and
                               result["completion"]["received"] ==
                               result["sender"]["sent"] and
                               result["completion_receiver"]["malformed"] == 0 and
                               result["completion_receiver"]["kernel_timestamped"] ==
                               result["sender"]["sent"] and
                               result["probe"]["bench_ack_sent"] ==
                               result["sender"]["sent"] and
                               result["probe"]["bench_ack_errors"] == 0)) and
                           abs(result["clock_drift_us"]) < 1000 and
                           (not args.completion_acks or (
                               abs(result["clock"]["bench_offset_ns"]) < 100_000 and
                               abs(result["clock_after"]["bench_offset_ns"]) < 100_000 and
                               abs(result["bench_clock_drift_us"]) < 100)) and
                           result["probe"]["history_records_attempted"] ==
                           result["probe"]["history_records_written"] and
                           result["server_exit"] == 0)
        result["correctness_valid"] = result["valid"]
        # A complete run can still be a bad load test when the sender falls
        # behind the 30-second schedule and later catches up in bursts.
        worst_second_p99_us = max(
            (second["p99_us"] for second in
             result["sender"]["worst_schedule_seconds"]), default=0)
        result["sender"]["worst_second_p99_us"] = worst_second_p99_us
        result["paced_valid"] = (
            result["sender"]["schedule_lateness"]["p99_us"] <= 1000 and
            worst_second_p99_us <= 10000 and
            result["sender"]["send_seconds"] <=
            result["sender"]["schedule_seconds"] + .010)
        result["valid"] = result["correctness_valid"] and result["paced_valid"]
    finally:
        if receiver and receiver.is_alive():
            if not ack_started:
                receiver_pipe.send(0)
            receiver_stop.set()
            await asyncio.to_thread(receiver.join, 5)
            if receiver.is_alive():
                receiver.terminate()
                await asyncio.to_thread(receiver.join, 5)
        if receiver_pipe:
            receiver_pipe.close()
        if native_receiver:
            if native_receiver.poll() is None:
                native_receiver.terminate()
                await asyncio.to_thread(native_receiver.wait)
            native_receiver.stdin.close()
            native_receiver.stdout.close()
        if native_receiver_stderr:
            native_receiver_stderr.close()
        if process and process.poll() is None:
            process.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(process.wait), 10)
            except asyncio.TimeoutError:
                process.kill()
                await asyncio.to_thread(process.wait)
        await wire.close()
        stdout.close()
        stderr.close()
        (state / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if not result.get("valid"):
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("case_dir", type=Path)
    parser.add_argument("--symbols", type=int, default=1000)
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--speed", type=float, default=1.0)
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--sample-start-seconds", type=float)
    parser.add_argument("--sample-duration-seconds", type=int, default=5)
    parser.add_argument("--prewarm-first-depth", action="store_true")
    parser.add_argument("--spread-symbols-us", type=int, default=0)
    parser.add_argument("--prebuild-frames", action="store_true")
    parser.add_argument("--disable-gc-during-send", action="store_true")
    parser.add_argument("--native-ack-receiver", action="store_true")
    parser.add_argument("--native-ws-sender", action="store_true")
    parser.add_argument("--kevent-probe", action="store_true")
    parser.add_argument("--strategy-refresh-ms", type=int)
    ack_mode = parser.add_mutually_exclusive_group()
    ack_mode.add_argument("--completion-acks", action="store_true")
    ack_mode.add_argument("--no-ack-control", action="store_true")
    arguments = parser.parse_args()
    if arguments.speed <= 0:
        parser.error("--speed must be positive")
    if not 0 <= arguments.spread_symbols_us <= 100_000:
        parser.error("--spread-symbols-us must be in [0,100000]")
    if arguments.native_ack_receiver and not arguments.completion_acks:
        parser.error("--native-ack-receiver requires --completion-acks")
    if arguments.native_ws_sender and not arguments.completion_acks:
        parser.error("--native-ws-sender requires --completion-acks")
    if arguments.kevent_probe and not (arguments.native_ws_sender and
                                       arguments.completion_acks and
                                       sys.platform == "darwin"):
        parser.error("--kevent-probe requires macOS, --native-ws-sender and --completion-acks")
    if arguments.strategy_refresh_ms is not None and \
            arguments.strategy_refresh_ms <= 0:
        parser.error("--strategy-refresh-ms must be positive")
    asyncio.run(run(arguments))
