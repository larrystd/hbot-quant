#!/usr/bin/env python3
"""One-shot, read-only Binance Spot Demo configuration check.

Public mode makes two REST requests. --market adds one depth snapshot and one
public WebSocket connection. --private adds two signed GET requests and one
private WebSocket subscription. It never sends an order or retries.
"""

import argparse
from decimal import Decimal
import hashlib
import hmac
import json
from pathlib import Path
import stat
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yaml


DEMO_REST = "demo-api.binance.com"
DEMO_MARKET_WS = "demo-stream.binance.com"
DEMO_PRIVATE_WS = "demo-ws-api.binance.com"


def read_config(path):
    config = yaml.safe_load(path.read_text())
    if (config.get("schema_version") != 2 or
            len(config.get("accounts", [])) != 1 or
            len(config.get("shards", [])) != 1):
        raise ValueError("Demo preflight requires one account and one shard")
    account = config["accounts"][0]
    market = config["shards"][0]["market"]
    source = config["input"]
    if (account.get("mode") != "online_sim" or
            account["trading_rest"] != {"host": DEMO_REST, "port": 443,
                                        "tls": True} or
            account["private_ws"] != {"host": DEMO_PRIVATE_WS,
                                       "port": 443, "tls": True} or
            source.get("source") != "binance_public" or
            source.get("rest_host") != DEMO_REST or
            source.get("websocket_host") != DEMO_MARKET_WS or
            source.get("rest_port") != 443 or
            source.get("websocket_port") != 443 or
            source.get("tls") is not True or
            source.get("depth_snapshot_limit") != 100 or
            source.get("max_snapshot_requests_per_run") != 3 or
            account.get("max_submissions_per_run") != 0):
        raise ValueError("preflight requires read-only, fixed Demo endpoints")
    symbol = market["symbol"]
    if not symbol.isalnum() or not symbol.isupper():
        raise ValueError("invalid Binance symbol")
    return account, market, path.parent


def read_credentials(account, config_dir):
    path = Path(account["api_credentials_file"])
    if not path.is_absolute():
        path = config_dir / path
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise ValueError("credentials must be a regular owner-only file (chmod 600)")
    if info.st_size > 4096:
        raise ValueError("credentials file is too large")
    credentials = yaml.safe_load(path.read_text())
    if (not isinstance(credentials, dict) or
            set(credentials) != {"api_key", "api_secret"} or
            not all(isinstance(value, str) and value and
                    value != "REPLACE_ME" for value in credentials.values())):
        raise ValueError("credentials file is incomplete")
    return credentials


def get_json(path, params=None, api_key=None):
    target = f"https://{DEMO_REST}{path}"
    if params:
        target += "?" + urlencode(params)
    headers = {"X-MBX-APIKEY": api_key} if api_key else {}
    try:
        with urlopen(Request(target, headers=headers), timeout=8) as response:
            return json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"Demo {path} returned HTTP {error.code}; no retry") from None
    except URLError as error:
        raise RuntimeError(f"Demo {path} connection failed: {error.reason}") from None


def signed_get(path, params, credentials):
    params = dict(params)
    params["recvWindow"] = 5000
    params["timestamp"] = int(time.time() * 1000)
    payload = urlencode(params)
    params["signature"] = hmac.new(credentials["api_secret"].encode(),
                                    payload.encode(), hashlib.sha256).hexdigest()
    return get_json(path, params, credentials["api_key"])


def check_private_stream(credentials):
    try:
        from websockets.sync.client import connect
    except ImportError as error:
        raise RuntimeError("install Python package websockets for private check") from error
    timestamp = int(time.time() * 1000)
    payload = f"apiKey={credentials['api_key']}&timestamp={timestamp}"
    signature = hmac.new(credentials["api_secret"].encode(), payload.encode(),
                         hashlib.sha256).hexdigest()
    request = {"id": "hquant-demo-preflight",
               "method": "userDataStream.subscribe.signature",
               "params": {"apiKey": credentials["api_key"],
                          "timestamp": timestamp, "signature": signature}}
    try:
        with connect(f"wss://{DEMO_PRIVATE_WS}/ws-api/v3", open_timeout=8,
                     close_timeout=2) as websocket:
            websocket.send(json.dumps(request))
            response = json.loads(websocket.recv(timeout=8))
    except Exception as error:
        raise RuntimeError("Demo private WebSocket check failed: " +
                           type(error).__name__) from None
    if response.get("id") != request["id"] or response.get("status") != 200:
        raise RuntimeError("Demo private subscription was rejected")


def check_public_market(symbol):
    snapshot = get_json("/api/v3/depth", {"symbol": symbol, "limit": 100})
    if (not isinstance(snapshot.get("lastUpdateId"), int) or
            not snapshot.get("bids") or not snapshot.get("asks")):
        raise RuntimeError("Demo depth snapshot is incomplete")
    try:
        from websockets.sync.client import connect
    except ImportError as error:
        raise RuntimeError("install Python package websockets for market check") from error
    lower = symbol.lower()
    url = (f"wss://{DEMO_MARKET_WS}/stream?streams="
           f"{lower}@depth/{lower}@trade")
    seen = set()
    try:
        with connect(url, open_timeout=8, close_timeout=2) as websocket:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and len(seen) < 2:
                frame = json.loads(websocket.recv(
                    timeout=max(0.1, deadline - time.monotonic())))
                event = frame.get("data", {})
                if not isinstance(event, dict) or event.get("s") != symbol:
                    raise RuntimeError("Demo market stream symbol mismatch")
                kind = event.get("e")
                if kind == "depthUpdate":
                    if (frame.get("stream") != f"{lower}@depth" or
                            not isinstance(event.get("U"), int) or
                            not isinstance(event.get("u"), int) or
                            event["U"] > event["u"]):
                        raise RuntimeError("Demo depth event is incomplete")
                    seen.add(kind)
                elif kind == "trade":
                    if (frame.get("stream") != f"{lower}@trade" or
                            Decimal(event.get("p", "0")) <= 0 or
                            Decimal(event.get("q", "0")) <= 0):
                        raise RuntimeError("Demo trade event is incomplete")
                    seen.add(kind)
    except RuntimeError:
        raise
    except Exception as error:
        raise RuntimeError("Demo public market WebSocket check failed: " +
                           type(error).__name__) from None
    if seen != {"depthUpdate", "trade"}:
        raise RuntimeError("Demo public market stream missed depth or trade")
    return {"snapshot": "ok", "snapshot_last_update_id":
            snapshot["lastUpdateId"], "market_ws_events": sorted(seen)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve()
                        .parents[1] / "examples/binance_demo.yaml")
    parser.add_argument("--private", action="store_true",
                        help="check signed account, open orders, and private WS")
    parser.add_argument("--market", action="store_true",
                        help="check one depth snapshot and public market WS")
    args = parser.parse_args()
    account, market, config_dir = read_config(args.config)
    symbol = market["symbol"]
    exchange = get_json("/api/v3/exchangeInfo", {"symbol": symbol})
    rows = exchange.get("symbols", [])
    if len(rows) != 1 or rows[0].get("symbol") != symbol or \
            rows[0].get("status") != "TRADING":
        raise RuntimeError("Demo symbol is unavailable")
    if (rows[0].get("baseAsset") != market["base_asset"] or
            rows[0].get("quoteAsset") != market["quote_asset"]):
        raise RuntimeError("Demo symbol assets differ from configuration")
    filters = {row["filterType"]: row for row in rows[0]["filters"]}
    venue_notional = filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {}))
    rules = market["trading_rule"]
    expected = (("price_increment", filters["PRICE_FILTER"]["tickSize"]),
                ("base_increment", filters["LOT_SIZE"]["stepSize"]),
                ("min_base_amount", filters["LOT_SIZE"]["minQty"]),
                ("min_order_value", venue_notional["minNotional"]))
    if any(Decimal(str(rules[field])) != Decimal(actual)
           for field, actual in expected):
        raise RuntimeError("Demo trading rules differ from configuration")
    server_time = get_json("/api/v3/time")["serverTime"]
    skew_ms = abs(server_time - int(time.time() * 1000))
    if skew_ms > 1000:
        raise RuntimeError(f"clock differs from Demo by {skew_ms} ms")
    result = {"symbol": symbol, "public_rules": "ok", "clock_skew_ms": skew_ms,
              "rest_requests": 2, "signed_requests": 0,
              "private_ws_subscriptions": 0, "market_ws_connections": 0,
              "orders_sent": 0}
    if args.market:
        result.update(check_public_market(symbol))
        result["rest_requests"] += 1
        result["market_ws_connections"] = 1
    if args.private:
        credentials = read_credentials(account, config_dir)
        balances = signed_get("/api/v3/account",
                              {"omitZeroBalances": "false"}, credentials)
        open_orders = signed_get("/api/v3/openOrders", {}, credentials)
        check_private_stream(credentials)
        totals = {row["asset"]: Decimal(row["free"]) +
                  Decimal(row["locked"]) for row in balances["balances"]}
        configured = {asset: Decimal(str(value)) for asset, value in
                      account["initial_balances"].items()}
        selected = {asset: str(totals.get(asset, Decimal(0))) for asset in
                    configured}
        unexpected = sorted(asset for asset, total in totals.items()
                            if asset not in configured and total != 0)
        result.update({"rest_requests": 4 + int(args.market), "signed_requests": 2,
                       "private_ws_subscriptions": 1,
                       "private_ws": "subscribed",
                       "account_totals": selected,
                       "unexpected_nonzero_assets": unexpected,
                       "unexpected_nonzero_totals": {
                           asset: str(totals[asset]) for asset in unexpected},
                       "configured_balances_match": all(
                           totals.get(asset, Decimal(0)) == value for
                           asset, value in configured.items()) and not unexpected,
                       "account_wide_open_orders": len(open_orders)})
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.private and (not result["configured_balances_match"] or
                         result["account_wide_open_orders"] != 0):
        raise RuntimeError("update configured balances or clear existing orders "
                           "before starting hquant")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, ValueError, OSError, RuntimeError, yaml.YAMLError) as error:
        print(f"preflight failed: {error}", file=sys.stderr)
        sys.exit(1)
