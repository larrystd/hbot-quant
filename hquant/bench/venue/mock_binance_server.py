"""Independent Binance Spot REST/WS user-data simulator for trade-chain tests."""

import asyncio
import base64
from collections import Counter
import hashlib
import hmac
import json
import time
from urllib.parse import parse_qs, urlsplit


SCALE = 1_000_000_000


def fixed(value):
    whole, _, fraction = value.partition(".")
    return int(whole) * SCALE + int(fraction.ljust(9, "0"))


def decimal(value):
    whole, fraction = divmod(value, SCALE)
    return f"{whole}.{fraction:09d}".rstrip("0").rstrip(".")


def frame(payload):
    data = json.dumps(payload, separators=(",", ":")).encode()
    if len(data) < 126:
        return bytes((0x81, len(data))) + data
    if len(data) < 65536:
        return bytes((0x81, 126)) + len(data).to_bytes(2, "big") + data
    return bytes((0x81, 127)) + len(data).to_bytes(8, "big") + data


async def read_frame(reader):
    header = await reader.readexactly(2)
    length = header[1] & 127
    if length == 126:
        length = int.from_bytes(await reader.readexactly(2), "big")
    elif length == 127:
        length = int.from_bytes(await reader.readexactly(8), "big")
    mask = await reader.readexactly(4) if header[1] & 128 else b"\0" * 4
    data = bytearray(await reader.readexactly(length))
    for index in range(length):
        data[index] ^= mask[index % 4]
    return json.loads(data)


class MockBinanceTrading:
    """Own order, fill and wallet ledger; no quantserver state is reused."""

    def __init__(self, symbols, balances, api_key="hquant-sim-key",
                 api_secret="hquant-sim-secret", fill_delay=0.05,
                 rules=None, fee_asset=None):
        self.symbols = symbols  # symbol -> (base asset, quote asset)
        self.wallet = {asset: {"free": fixed(amount), "locked": 0}
                       for asset, amount in balances.items()}
        self.api_key = api_key
        self.api_secret = api_secret
        self.fill_delay = fill_delay
        self.fee_asset = fee_asset
        self.rules = rules or {
            symbol: {"price_increment": "0.01", "base_increment": "0.001",
                     "min_base_amount": "0.001", "min_order_value": "5"}
            for symbol in symbols}
        self.orders = {}
        self.order_locks = {}
        self.trades = []
        self.received = []
        self.sent_events = []
        self.rest_server = None
        self.ws_server = None
        self.ws_writer = None
        self.ws_write_lock = asyncio.Lock()
        self.ledger_lock = asyncio.Lock()
        self.ws_ready = asyncio.Event()
        self.private_connections = 0
        self.drop_submit_response_once = False
        self.timeout_submit_response_once = False
        self.timeout_cancel_without_execution_once = False
        self.rate_limit_submit_once = False
        self.rate_limited_at_ns = 0
        self.suppress_private_events = False
        self.next_order_id = 1
        self.next_trade_id = 1
        self.next_event_ms = int(time.time() * 1000)
        self.accepted_count = 0
        self.execution_counts = Counter()
        self.cancel_fill_races = 0
        self.duplicate_reports = 0
        self.tasks = set()

    async def start(self):
        self.rest_server = await asyncio.start_server(self._http, "127.0.0.1", 0)
        self.ws_server = await asyncio.start_server(self._websocket, "127.0.0.1", 0)
        return (self.rest_server.sockets[0].getsockname()[1],
                self.ws_server.sockets[0].getsockname()[1])

    async def close(self):
        for task in tuple(self.tasks):
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        for server in (self.rest_server, self.ws_server):
            if server:
                server.close()
                await server.wait_closed()
        if self.ws_writer:
            self.ws_writer.close()
            await self.ws_writer.wait_closed()

    async def disconnect_private(self):
        if self.ws_writer:
            self.ws_writer.close()
            try:
                await self.ws_writer.wait_closed()
            except ConnectionError:
                pass

    def snapshot(self):
        return {
            "orders": {key: dict(order) for key, order in self.orders.items()},
            "trades": list(self.trades),
            "wallet": {key: dict(value) for key, value in self.wallet.items()},
            "received": list(self.received),
            "sent_events": list(self.sent_events),
            "execution_counts": dict(self.execution_counts),
            "cancel_fill_races": self.cancel_fill_races,
            "duplicate_reports": self.duplicate_reports,
        }

    def _signed(self, path, headers):
        if headers.get("x-mbx-apikey") != self.api_key:
            return False
        query = urlsplit(path).query
        prefix, _, signature = query.rpartition("&signature=")
        if not signature:
            return False
        expected = hmac.new(self.api_secret.encode(), prefix.encode(),
                            hashlib.sha256).hexdigest()
        return hmac.compare_digest(signature, expected)

    async def _http(self, reader, writer):
        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break
                method, path, _ = line.decode().strip().split(" ", 2)
                headers = {}
                while True:
                    line = await reader.readline()
                    if line == b"\r\n":
                        break
                    key, value = line.decode().split(":", 1)
                    headers[key.lower()] = value.strip()
                length = int(headers.get("content-length", "0"))
                if length:
                    await reader.readexactly(length)
                endpoint = urlsplit(path).path
                extra_headers = ""
                if method == "GET" and endpoint == "/api/v3/exchangeInfo":
                    code, payload = 200, self._exchange_info()
                elif not self._signed(path, headers):
                    code, payload = 401, {"code": -1022, "msg": "Bad signature"}
                else:
                    values = parse_qs(urlsplit(path).query)
                    if (method == "POST" and endpoint == "/api/v3/order" and
                            self.rate_limit_submit_once):
                        self.rate_limit_submit_once = False
                        self.rate_limited_at_ns = time.clock_gettime_ns(
                            time.CLOCK_MONOTONIC)
                        code, payload = 429, {"code": -1003, "msg": "Too many requests"}
                        extra_headers = "Retry-After: 1\r\n"
                    elif method == "POST" and endpoint == "/api/v3/order":
                        code, payload = await self._place(values)
                        if self.timeout_submit_response_once:
                            self.timeout_submit_response_once = False
                            code, payload = 400, {
                                "code": -1007,
                                "msg": "Send status unknown; execution status unknown",
                            }
                    elif method == "DELETE" and endpoint == "/api/v3/order":
                        code, payload = await self._cancel(values)
                    elif method == "GET" and endpoint == "/api/v3/order":
                        async with self.ledger_lock:
                            code, payload = self._query(values)
                    elif method == "GET" and endpoint == "/api/v3/openOrders":
                        async with self.ledger_lock:
                            code, payload = 200, [self._order_json(order)
                                                  for order in self.orders.values()
                                                  if order["status"] in
                                                  ("NEW", "PARTIALLY_FILLED")]
                    elif method == "GET" and endpoint == "/api/v3/account":
                        async with self.ledger_lock:
                            code, payload = 200, {"balances": [
                                {"asset": asset, "free": decimal(balance["free"]),
                                 "locked": decimal(balance["locked"])}
                                for asset, balance in self.wallet.items()]}
                    elif method == "GET" and endpoint == "/api/v3/myTrades":
                        async with self.ledger_lock:
                            code, payload = self._my_trades(values)
                    else:
                        code, payload = 404, {"code": -1000, "msg": "Unknown path"}
                if (method == "POST" and endpoint == "/api/v3/order" and
                        self.drop_submit_response_once):
                    self.drop_submit_response_once = False
                    return
                data = json.dumps(payload, separators=(",", ":")).encode()
                writer.write(f"HTTP/1.1 {code} {'OK' if code == 200 else 'Error'}\r\n"
                             "Content-Type: application/json\r\n"
                             f"Content-Length: {len(data)}\r\n"
                             f"{extra_headers}"
                             "Connection: keep-alive\r\n\r\n".encode() + data)
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    @staticmethod
    def _one(values, key):
        return values.get(key, [""])[0]

    def _exchange_info(self):
        symbols = []
        for symbol, (base, quote) in self.symbols.items():
            rule = self.rules[symbol]
            symbols.append({
                "symbol": symbol, "status": "TRADING",
                "baseAsset": base, "quoteAsset": quote,
                "filters": [
                    {"filterType": "PRICE_FILTER", "minPrice": "0",
                     "maxPrice": "0", "tickSize": rule["price_increment"]},
                    {"filterType": "LOT_SIZE",
                     "minQty": rule["min_base_amount"],
                     "maxQty": "1000000000",
                     "stepSize": rule["base_increment"]},
                    {"filterType": "MIN_NOTIONAL",
                     "minNotional": rule["min_order_value"]},
                ],
            })
        return {"rateLimits": [], "symbols": symbols}

    def _order_json(self, order, cancel=False):
        response = {
            "symbol": order["symbol"], "orderId": order["exchange_id"],
            "clientOrderId": order["client_id"],
            "price": decimal(order["price"]),
            "origQty": decimal(order["quantity"]),
            "executedQty": decimal(order["filled"]),
            "status": order["status"], "side": order["side"],
            "updateTime": order.get("update_ms", 0),
        }
        if cancel:
            response["origClientOrderId"] = order["client_id"]
        return response

    async def _place(self, values):
        async with self.ledger_lock:
            return await self._place_locked(values)

    async def _place_locked(self, values):
        symbol = self._one(values, "symbol")
        client_id = self._one(values, "newClientOrderId")
        side = self._one(values, "side")
        try:
            quantity = fixed(self._one(values, "quantity"))
            price = fixed(self._one(values, "price"))
        except ValueError:
            return 400, {"code": -1013, "msg": "Invalid quantity or price"}
        if (symbol not in self.symbols or not client_id or
                side not in ("BUY", "SELL") or
                self._one(values, "type") != "LIMIT" or
                self._one(values, "timeInForce") != "GTC" or
                quantity <= 0 or price <= 0 or client_id in self.orders):
            return 400, {"code": -1013, "msg": "Invalid order"}
        base, quote = self.symbols[symbol]
        asset = quote if side == "BUY" else base
        lock = (price * quantity + SCALE - 1) // SCALE if side == "BUY" else quantity
        self.received.append({"kind": "submit", "client_id": client_id,
                              "symbol": symbol, "price": price,
                              "at_ns": time.clock_gettime_ns(time.CLOCK_MONOTONIC)})
        if self.wallet[asset]["free"] < lock or len(self.received) % 11 == 0:
            rejected = {"client_id": client_id, "exchange_id": 0,
                        "symbol": symbol, "side": side, "price": price,
                        "quantity": quantity, "filled": 0,
                        "status": "REJECTED"}
            await self._execution(rejected, "REJECTED")
            return 400, {"code": -2010, "msg": "Account has insufficient balance"}
        self.wallet[asset]["free"] -= lock
        self.wallet[asset]["locked"] += lock
        order = {"client_id": client_id, "exchange_id": self.next_order_id,
                 "symbol": symbol, "side": side, "price": price,
                 "quantity": quantity, "filled": 0, "status": "NEW",
                 "lock": lock}
        self.next_order_id += 1
        self.accepted_count += 1
        self.orders[client_id] = order
        self.order_locks[client_id] = asyncio.Lock()
        async with self.order_locks[client_id]:
            await self._execution(order, "NEW")
            await self._balances([asset])
        task = asyncio.create_task(self._script_fill(client_id,
                                                     self.accepted_count % 2 == 0))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return 200, self._order_json(order)

    async def _cancel(self, values):
        async with self.ledger_lock:
            return await self._cancel_locked(values)

    async def _cancel_locked(self, values):
        client_id = self._one(values, "origClientOrderId")
        order = self.orders.get(client_id)
        self.received.append({"kind": "cancel", "client_id": client_id,
                              "symbol": self._one(values, "symbol"),
                              "at_ns": time.clock_gettime_ns(time.CLOCK_MONOTONIC)})
        if self.timeout_cancel_without_execution_once:
            self.timeout_cancel_without_execution_once = False
            return 503, {"code": -1007,
                         "msg": "Send status unknown; execution status unknown"}
        if not order or order["symbol"] != self._one(values, "symbol"):
            return 400, {"code": -2011, "msg": "Unknown order sent"}
        async with self.order_locks[client_id]:
            if order["status"] not in ("NEW", "PARTIALLY_FILLED"):
                return 400, {"code": -2011, "msg": "Unknown order sent"}
            # The remaining quantity can trade while a cancel is in flight.
            remaining = order["quantity"] - order["filled"]
            if order["exchange_id"] % 7 == 0 and remaining > 1:
                await self._fill(order, remaining // 2)
                self.cancel_fill_races += 1
            base, quote = self.symbols[order["symbol"]]
            asset = quote if order["side"] == "BUY" else base
            self.wallet[asset]["locked"] -= order["lock"]
            self.wallet[asset]["free"] += order["lock"]
            order["lock"] = 0
            order["status"] = "CANCELED"
            await self._execution(order, "CANCELED")
            await self._balances([asset])
            return 200, self._order_json(order, cancel=True)

    def _query(self, values):
        client_id = self._one(values, "origClientOrderId")
        order = self.orders.get(client_id)
        self.received.append({"kind": "query", "client_id": client_id,
                              "symbol": self._one(values, "symbol"),
                              "at_ns": time.clock_gettime_ns(time.CLOCK_MONOTONIC)})
        if not order or order["symbol"] != self._one(values, "symbol"):
            return 400, {"code": -2013, "msg": "Order does not exist"}
        return 200, self._order_json(order)

    def _my_trades(self, values):
        symbol = self._one(values, "symbol")
        try:
            exchange_id = int(self._one(values, "orderId"))
        except ValueError:
            return 400, {"code": -1102, "msg": "orderId required"}
        result = []
        try:
            from_id = int(self._one(values, "fromId")) if self._one(values, "fromId") else 0
            limit = int(self._one(values, "limit")) if self._one(values, "limit") else 500
        except ValueError:
            return 400, {"code": -1102, "msg": "invalid trade cursor"}
        if limit <= 0 or limit > 1000:
            return 400, {"code": -1102, "msg": "invalid trade limit"}
        for trade in self.trades:
            order = self.orders[trade["client_id"]]
            if (order["symbol"] != symbol or
                    order["exchange_id"] != exchange_id or
                    trade["trade_id"] < from_id):
                continue
            gross = trade["price"] * trade["quantity"] // SCALE
            result.append({"symbol": symbol, "id": trade["trade_id"],
                           "orderId": exchange_id,
                           "price": decimal(trade["price"]),
                           "qty": decimal(trade["quantity"]),
                           "quoteQty": decimal(gross),
                           "commission": decimal(trade["fee"]),
                           "commissionAsset": trade["fee_asset"],
                           "time": trade["time_ms"],
                           "isBuyer": order["side"] == "BUY"})
        return 200, result[:limit]

    async def _script_fill(self, client_id, full):
        await asyncio.sleep(self.fill_delay)
        order = self.orders.get(client_id)
        if not order:
            return
        async with self.ledger_lock:
            async with self.order_locks[client_id]:
                if order["status"] != "NEW":
                    return
                quantity = order["quantity"] if full else max(1, order["quantity"] // 2)
                await self._fill(order, quantity)

    async def _fill(self, order, quantity):
        base, quote = self.symbols[order["symbol"]]
        gross = order["price"] * quantity // SCALE
        fee_asset = self.fee_asset or quote
        fee = ((gross + 999) // 1000 if fee_asset == quote
               else fixed("0.00001"))
        if order["side"] == "BUY":
            self.wallet[quote]["locked"] -= gross
            order["lock"] -= gross
            self.wallet[base]["free"] += quantity
        else:
            self.wallet[base]["locked"] -= quantity
            order["lock"] -= quantity
            self.wallet[quote]["free"] += gross
        self.wallet[fee_asset]["free"] -= fee
        order["filled"] += quantity
        order["status"] = ("FILLED" if order["filled"] == order["quantity"]
                           else "PARTIALLY_FILLED")
        trade_id = self.next_trade_id
        self.next_trade_id += 1
        trade = {"client_id": order["client_id"],
                 "trade_id": trade_id, "symbol": order["symbol"],
                 "quantity": quantity, "price": order["price"],
                 "fee": fee, "fee_asset": fee_asset}
        self.trades.append(trade)
        await self._execution(order, "TRADE", quantity, order["price"],
                              fee, fee_asset, trade_id)
        trade["time_ms"] = order["update_ms"]
        await self._balances(list(dict.fromkeys([base, quote, fee_asset])))

    async def _execution(self, order, execution, last_quantity=0,
                         last_price=0, fee=0, fee_asset=None, trade_id=-1):
        self.execution_counts[order["status"]] += 1
        self.next_event_ms = max(self.next_event_ms + 1,
                                 int(time.time() * 1000))
        order["update_ms"] = self.next_event_ms
        payload = {
            "e": "executionReport", "E": self.next_event_ms,
            "T": self.next_event_ms, "s": order["symbol"],
            "c": order["client_id"], "S": order["side"], "o": "LIMIT",
            "f": "GTC", "q": decimal(order["quantity"]),
            "p": decimal(order["price"]), "C": "", "x": execution,
            "X": order["status"], "r": "NONE", "i": order["exchange_id"],
            "l": decimal(last_quantity), "z": decimal(order["filled"]),
            "L": decimal(last_price), "n": decimal(fee), "N": fee_asset,
            "t": trade_id, "Y": decimal(last_price * last_quantity // SCALE),
        }
        await self._send_event(payload)
        if execution == "TRADE" and order["exchange_id"] % 17 == 0:
            await self._send_event(dict(payload))
            self.duplicate_reports += 1

    async def _balances(self, assets):
        self.next_event_ms = max(self.next_event_ms + 1,
                                 int(time.time() * 1000))
        await self._send_event({
            "e": "outboundAccountPosition", "E": self.next_event_ms,
            "u": self.next_event_ms,
            "B": [{"a": asset, "f": decimal(self.wallet[asset]["free"]),
                   "l": decimal(self.wallet[asset]["locked"])}
                  for asset in assets],
        })

    async def _send_event(self, event):
        # REST handlers and scripted fills run in separate asyncio tasks.
        # StreamWriter.drain() permits only one waiter per socket.
        async with self.ws_write_lock:
            self.sent_events.append({"event": event,
                                     "at_ns": time.clock_gettime_ns(
                                         time.CLOCK_MONOTONIC)})
            if self.ws_writer and not self.suppress_private_events:
                self.ws_writer.write(frame({"subscriptionId": 0,
                                            "event": event}))
                await self.ws_writer.drain()

    async def _websocket(self, reader, writer):
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            lines = request.decode().split("\r\n")
            if not lines[0].startswith("GET /ws-api/v3 "):
                writer.close()
                return
            key = next(line.split(":", 1)[1].strip() for line in lines
                       if line.lower().startswith("sec-websocket-key:"))
            accept = base64.b64encode(hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
            ).digest()).decode()
            writer.write(("HTTP/1.1 101 Switching Protocols\r\n"
                          "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                          f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode())
            await writer.drain()
            request = await read_frame(reader)
            params = request.get("params", {})
            signature = params.get("signature", "")
            timestamp = params.get("timestamp")
            expected = hmac.new(self.api_secret.encode(),
                                f"apiKey={self.api_key}&timestamp={timestamp}".encode(),
                                hashlib.sha256).hexdigest()
            valid = (request.get("method") ==
                     "userDataStream.subscribe.signature" and
                     params.get("apiKey") == self.api_key and
                     hmac.compare_digest(signature, expected))
            writer.write(frame({"id": request.get("id"),
                                "status": 200 if valid else 400,
                                "result": {"subscriptionId": 0} if valid else {}}))
            await writer.drain()
            if not valid:
                return
            self.ws_writer = writer
            self.private_connections += 1
            self.ws_ready.set()
            await self._balances(list(self.wallet))
            while not reader.at_eof():
                await read_frame(reader)
        except (ConnectionError, asyncio.IncompleteReadError, StopIteration):
            pass
        finally:
            if self.ws_writer is writer:
                self.ws_writer = None
                self.ws_ready.clear()
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
