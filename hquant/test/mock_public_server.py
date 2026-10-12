#!/usr/bin/env python3
"""Local public market endpoints shared by external trade-chain tests."""

import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import queue
import socket
import threading
import time


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


