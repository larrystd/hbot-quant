"""Cross-check the simulator ledger, quantserver account events and timing IDs."""

from collections import Counter, defaultdict, deque
from pathlib import Path
import sqlite3
import struct

from market.market_wire import distribution


def decode_journal(payload):
    if not payload or payload[0] != 1 or len(payload) < 1 + 19 * 8:
        raise ValueError("invalid journal record header")
    fields = struct.unpack_from("<19Q", payload, 1)
    offset = 1 + 19 * 8
    values = []
    for _ in range(5):
        if offset + 8 > len(payload):
            raise ValueError("truncated journal string length")
        length = struct.unpack_from("<Q", payload, offset)[0]
        offset += 8
        if offset + length > len(payload):
            raise ValueError("truncated journal string")
        values.append(payload[offset:offset + length].decode())
        offset += length
    if offset != len(payload):
        raise ValueError("trailing journal data")
    return {"client_id": values[0], "trade_id": fields[7]}


def validate(database, venue, sent_ns, duration_seconds, final_accounts):
    errors = []
    with sqlite3.connect(database) as db:
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            errors.append("history SQLite integrity check failed")
        db.row_factory = sqlite3.Row
        rows = [dict(row) for row in db.execute(
            "SELECT * FROM account_events ORDER BY account_sequence")]
        gaps = db.execute("SELECT COUNT(*) FROM history_gaps").fetchone()[0]
    if gaps:
        errors.append(f"history has {gaps} gaps")
    if [row["account_sequence"] for row in rows] != list(range(1, len(rows) + 1)):
        errors.append("account history sequence is not continuous")
    if len({row["run_id"] for row in rows}) != 1:
        errors.append("account history run ID is missing or mixed")
    if len({row["account_id"] for row in rows}) != 1:
        errors.append("account history account ID is missing or mixed")
    with sqlite3.connect(Path(database).with_name("order_journal.sqlite")) as db:
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            errors.append("order journal SQLite integrity check failed")
        journal_rows = db.execute(
            "SELECT account,sequence,kind,payload FROM events "
            "ORDER BY account,sequence").fetchall()
    if [row[1] for row in journal_rows] != list(range(1, len(journal_rows) + 1)):
        errors.append("order journal sequence is not continuous")
    if any(row[0] != "shared" or not 0 <= row[2] <= 12
           for row in journal_rows):
        errors.append("order journal account or event kind is invalid")
    journal_by_kind = defaultdict(list)
    for _, _, kind, payload in journal_rows:
        try:
            journal_by_kind[kind].append(decode_journal(payload))
        except (UnicodeError, ValueError, struct.error) as exc:
            errors.append(f"invalid order journal payload: {exc}")
            break

    by_kind = defaultdict(list)
    by_client = defaultdict(lambda: defaultdict(list))
    for row in rows:
        by_kind[row["kind"]].append(row)
        if row["client_order_id"]:
            by_client[row["client_order_id"]][row["kind"]].append(row)

    for kind, name in ((1, "submit"), (2, "cancel")):
        local = Counter(row["client_order_id"] for row in by_kind[kind])
        remote = Counter(row["client_id"] for row in venue["received"]
                         if row["kind"] == name)
        if local != remote:
            errors.append(f"{name} IDs differ between account and socket")
    for journal_kind, history_kind, name in (
            (0, 1, "submit intent"), (2, 2, "cancel intent"),
            (11, 15, "submit write"), (12, 16, "cancel write")):
        journal_ids = Counter(row["client_id"] for row in
                              journal_by_kind[journal_kind])
        history_ids = Counter(row["client_order_id"] for row in
                              by_kind[history_kind])
        if journal_ids != history_ids:
            errors.append(f"{name} IDs differ between journal and history")
    for row in by_kind[10]:
        client = row["client_order_id"]
        if not any(by_client[client][kind] for kind in (3, 4, 5, 8)):
            errors.append(f"{client}: submit timeout remains unresolved")
    for row in by_kind[11]:
        client = row["client_order_id"]
        if not any(by_client[client][kind] for kind in (4, 5, 12)):
            errors.append(f"{client}: cancel timeout remains unresolved")
    if by_kind[13]:
        errors.append("locally rejected command during stress run")
    if any(order["status"] in ("NEW", "PARTIALLY_FILLED")
           for order in venue["orders"].values()):
        errors.append("simulated venue has open orders after shutdown")

    for client, order in venue["orders"].items():
        local = by_client[client]
        if len(local[1]) != 1:
            errors.append(f"{client}: submit command count {len(local[1])}")
        elif (local[1][0]["symbol"] != order["symbol"] or
              int(local[1][0]["price_nanos"]) != order["price"] or
              int(local[1][0]["quantity_nanos"]) != order["quantity"]):
            errors.append(f"{client}: submitted symbol, price or quantity mismatch")
        if len(local[3]) != 1:
            errors.append(f"{client}: accepted count {len(local[3])}")
        terminal_kind = {"FILLED": 4, "CANCELED": 5}.get(order["status"])
        if terminal_kind and len(local[terminal_kind]) != 1:
            errors.append(f"{client}: terminal status missing")
        elif terminal_kind and int(local[terminal_kind][0]["remaining_nanos"]) != \
                order["quantity"] - order["filled"]:
            errors.append(f"{client}: terminal remaining quantity mismatch")
        if local[3] and int(local[3][0]["exchange_order_id"]) != order["exchange_id"]:
            errors.append(f"{client}: exchange order ID mismatch")
    rejected = {row["client_id"] for row in venue["received"]
                if row["kind"] == "submit" and
                row["client_id"] not in venue["orders"]}
    if rejected != {row["client_order_id"] for row in by_kind[8]}:
        errors.append("explicit rejected order IDs differ")

    local_trades = {int(row["trade_id"]): row for row in by_kind[6]}
    journal_trades = {row["trade_id"] for row in journal_by_kind[7]}
    if journal_trades != {trade["trade_id"] for trade in venue["trades"]}:
        errors.append("fill IDs differ between journal and simulated venue")
    if len(local_trades) != len(by_kind[6]) or len(local_trades) != len(venue["trades"]):
        errors.append("trade IDs are missing or duplicated")
    for trade in venue["trades"]:
        row = local_trades.get(trade["trade_id"])
        if not row:
            errors.append(f"trade {trade['trade_id']} missing")
            continue
        for field, expected in (("client_order_id", trade["client_id"]),
                                ("symbol", trade["symbol"]),
                                ("quantity_nanos", trade["quantity"]),
                                ("price_nanos", trade["price"]),
                                ("fee_nanos", trade["fee"]),
                                ("asset", trade["fee_asset"])):
            actual = row[field] if isinstance(expected, str) else int(row[field])
            if actual != expected:
                errors.append(f"trade {trade['trade_id']} {field} mismatch")

    last_balances = {}
    for row in by_kind[7]:
        last_balances[row["asset"]] = row
    for asset, balance in venue["wallet"].items():
        row = last_balances.get(asset)
        if not row:
            errors.append(f"{asset}: balance event missing")
            continue
        for field, expected in (("available_nanos", balance["free"]),
                                ("locked_nanos", balance["locked"]),
                                ("total_nanos", balance["free"] + balance["locked"])):
            if int(row[field]) != expected:
                errors.append(f"{asset}: {field} mismatch")

    if len(final_accounts) != 1 or final_accounts[0]["id"] != "shared":
        errors.append("final account snapshot missing or not shared")
    else:
        account = final_accounts[0]
        if (not account["ready"] or account["error"] or
                account["queued"] or account["open_orders"] or
                account["unknown_orders"]):
            errors.append("final account has pending work, orders or error")
        if any(key not in account for key in
               ("pending_journal", "account_queue_peak",
                "account_queue_rejections", "journal_pending_peak",
                "journal_rejections", "trade_queue")):
            errors.append("final account queue health is missing")
        if (account.get("pending_journal", 0) or
                account.get("account_queue_rejections", 0) or
                account.get("journal_rejections", 0) or
                account.get("trade_queue", {}).get("queued", 0) or
                account.get("trade_queue", {}).get("rejected", 0)):
            errors.append("account, journal or trade queue did not drain cleanly")
        if (account.get("account_queue_peak", 0) > 8192 or
                account.get("trade_queue", {}).get("peak", 0) > 8192):
            errors.append("account or trade queue exceeded configured bound")
        if set(account["balances"]) != set(venue["wallet"]):
            errors.append("final account asset set differs from simulated venue")
        for asset, balance in venue["wallet"].items():
            local = account["balances"].get(asset)
            if not local:
                continue
            expected = {"total_nanos": balance["free"] + balance["locked"],
                        "free_nanos": balance["free"],
                        "locked_nanos": balance["locked"],
                        "available_nanos": balance["free"],
                        "reserved_nanos": 0}
            for field, value in expected.items():
                if local[field] != value:
                    errors.append(f"{asset}: final account {field} mismatch")

    sent_by_id = {row["client_id"]: row for row in venue["received"]
                  if row["kind"] == "submit"}
    sent_cancel_by_id = {row["client_id"]: row for row in venue["received"]
                         if row["kind"] == "cancel"}
    market_to_order = []
    for kind, sent in ((1, sent_by_id), (2, sent_cancel_by_id)):
        for row in by_kind[kind]:
            sequence = int(row["source_bench_seq"])
            if not sequence or sequence >= len(sent_ns):
                continue
            received = sent.get(row["client_order_id"])
            if received:
                elapsed = received["at_ns"] - sent_ns[sequence]
                if elapsed >= 0:
                    market_to_order.append(elapsed)
                else:
                    errors.append("negative market to order latency")

    reports = defaultdict(deque)
    for item in venue["sent_events"]:
        event = item["event"]
        if event["e"] == "executionReport":
            reports[(event["c"], event["X"])].append(item["at_ns"])
    order_status = {"REJECTED": 1, "NEW": 2, "PARTIALLY_FILLED": 3,
                    "FILLED": 4, "CANCELED": 5}
    inverse_status = {str(value): key for key, value in order_status.items()}
    report_to_apply = []
    full_chain = []
    market_to_fill = []
    completed_clients = set()
    source_by_client = {row["client_order_id"]: int(row["source_bench_seq"])
                        for row in by_kind[1]}
    for row in by_kind[14]:
        client = row["client_order_id"]
        status = inverse_status.get(row["message"])
        queue = reports[(client, status)]
        if not queue:
            errors.append(f"{client}: private report missing at simulated venue")
            continue
        sent_at = queue.popleft()
        applied_at = int(row["at_mono_ns"])
        if applied_at < sent_at:
            errors.append("negative private report latency")
            continue
        report_to_apply.append(applied_at - sent_at)
        source = source_by_client.get(client, 0)
        if source and source < len(sent_ns) and status in (
                "NEW", "PARTIALLY_FILLED", "FILLED") and \
                client not in completed_clients:
            elapsed = applied_at - sent_ns[source]
            if elapsed >= 0:
                full_chain.append(elapsed)
                completed_clients.add(client)

    for trade in venue["trades"]:
        source = source_by_client.get(trade["client_id"], 0)
        row = local_trades.get(trade["trade_id"])
        if source and source < len(sent_ns) and row:
            elapsed = int(row["at_mono_ns"]) - sent_ns[source]
            if elapsed >= 0:
                market_to_fill.append(elapsed)

    result = {
        "account_events": len(rows),
        "journal_events": len(journal_rows),
        "submits": len(by_kind[1]),
        "cancels": len(by_kind[2]),
        "fills": len(by_kind[6]),
        "private_reports": len(by_kind[14]),
        "script_coverage": {**venue["execution_counts"],
                            "cancel_fill_races": venue["cancel_fill_races"],
                            "duplicate_reports": venue["duplicate_reports"]},
        "market_to_order": distribution(market_to_order),
        "private_report_to_apply": distribution(report_to_apply),
        "market_to_private_report": distribution(full_chain),
        "market_to_fill": distribution(market_to_fill),
        "qps": {"submits": len(by_kind[1]) / duration_seconds,
                "cancels": len(by_kind[2]) / duration_seconds,
                "fills": len(by_kind[6]) / duration_seconds,
                "private_reports": len(by_kind[14]) / duration_seconds},
        "errors": errors[:50],
    }
    return result
