#!/usr/bin/env python3
"""Repeat release benchmarks, verify SQLite completeness, save raw metrics."""

import argparse
import json
import platform
import re
import sqlite3
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

from make_replay import write_workload


ROOT = Path(__file__).resolve().parents[2]
CASES = {
    "market1": ("market", 100_000, 1),
    "market4": ("market", 100_000, 4),
    "refresh1": ("refresh", 20_000, 1),
    "mixed4": ("mixed", 100_000, 4),
}


def execute(command):
    wrapped = ["/usr/bin/time", "-l", *map(str, command)] if platform.system() == "Darwin" else list(map(str, command))
    begin = time.perf_counter()
    result = subprocess.run(wrapped, cwd=ROOT, capture_output=True,
                            text=True, timeout=120)
    elapsed = time.perf_counter() - begin
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {command}\n{result.stderr[-3000:]}")
    metrics = {"process_wall_seconds": elapsed}
    if platform.system() == "Darwin":
        match = re.search(r"([0-9.]+) real\s+([0-9.]+) user\s+([0-9.]+) sys", result.stderr)
        if match:
            metrics["time_real_seconds"], metrics["user_seconds"], metrics["sys_seconds"] = map(float, match.groups())
        match = re.search(r"(\d+)\s+maximum resident set size", result.stderr)
        if match:
            metrics["max_rss_bytes"] = int(match.group(1))
    return result.stdout, metrics


def history_check(state):
    with sqlite3.connect(state / "history.sqlite") as db:
        count = db.execute("SELECT COUNT(*) FROM history_records").fetchone()[0]
        gaps = db.execute("SELECT COUNT(*) FROM history_gaps").fetchone()[0]
        by_shard = db.execute(
            "SELECT shard, MIN(shard_sequence), MAX(shard_sequence), COUNT(*) "
            "FROM history_records GROUP BY shard ORDER BY shard"
        ).fetchall()
        kinds = dict(db.execute(
            "SELECT kind, COUNT(*) FROM history_records GROUP BY kind"
        ).fetchall())
    if gaps or any(first != 1 or last != rows for _, first, last, rows in by_shard):
        raise RuntimeError(f"history incomplete: rows={count}, gaps={gaps}, shards={by_shard}")
    return {"rows": count, "gaps": gaps, "by_shard": by_shard,
            "kinds": kinds}


def expected_history(name):
    workload, events, shards = CASES[name]
    if workload == "market":
        cycles = 0
    elif workload == "refresh":
        cycles = events
    else:
        # Each shard sees one input every 6.000004 simulated seconds; a
        # 15-second refresh therefore fires on every third input.
        assert shards == 4 and events % shards == 0
        cycles = shards * (events // shards // 3)
    round_trips = shards + cycles
    cancellations = shards + cycles  # shutdown cancels the final quote
    kinds = {1: 2 * round_trips, 2: 2 * cancellations,
             3: 2 * round_trips, 5: 2 * cancellations,
             7: 4 * shards + 6 * cycles}
    return {"rows": sum(kinds.values()), "kinds": kinds}


def require_expected_history(name, history):
    expected = expected_history(name)
    if history["rows"] != expected["rows"] or history["kinds"] != expected["kinds"]:
        raise RuntimeError(f"unexpected history in {name}: {history}, expected {expected}")


def run_case(name, fixture, repeats, bench, server):
    configuration = fixture / "config.yaml"
    data = {"workload": name, "fixture_bytes": (fixture / "replay.json").stat().st_size,
            "benchmark_runs": []}
    for repeat in range(repeats):
        with tempfile.TemporaryDirectory(prefix=f"hquant-{name}-bench-") as temporary:
            state = Path(temporary)
            stdout, resource = execute([bench, configuration, state])
            result = json.loads(stdout)
            history = history_check(state)
            require_expected_history(name, history)
            if (result["shard_inputs"] != result["deliveries"] or
                    result["deliveries"] != CASES[name][1] + 3 * CASES[name][2]):
                raise RuntimeError(f"missing shard input in {name}: {result}")
            for stage in ("roundtrip", "post_to_shard", "input_handler",
                          "handler_to_completion", "callback_to_get"):
                if result[stage]["count"] != result["deliveries"]:
                    raise RuntimeError(f"incomplete {stage} timing in {name}: {result}")
            if (result["history_enqueue"]["count"] != result["history_records_attempted"] or
                    result["account_event"]["count"] != result["account_events"] or
                    result["sqlite_batch_write"]["count"] != result["history_batches"]):
                raise RuntimeError(f"incomplete stage timing in {name}: {result}")
            if history["rows"] != result["history_records_written"] or result["history_records_written"] != result["history_records_attempted"]:
                raise RuntimeError(f"history mismatch in {name} run {repeat}: {result}")
            data["benchmark_runs"].append({**result, **resource, "history": history})
            print(f"{name} bench {repeat + 1}/{repeats}: {result['replay_qps']:.0f} qps, p99 {result['roundtrip']['p99_us']:.1f} us, commit p99 {result['input_to_commit']['p99_us']:.1f} us", flush=True)
    data["production_runs"] = []
    for repeat in range(repeats):
        with tempfile.TemporaryDirectory(prefix=f"hquant-{name}-server-") as temporary:
            state = Path(temporary)
            _, resource = execute([server, f"--config={configuration}",
                                   f"--state_dir={state}"])
            history = history_check(state)
            require_expected_history(name, history)
            if history["rows"] != data["benchmark_runs"][0]["history"]["rows"]:
                raise RuntimeError(f"production row count differs for {name}: {history}")
            deliveries = data["benchmark_runs"][0]["deliveries"]
            production = {**resource, "history": history,
                          "process_qps": deliveries / resource["process_wall_seconds"]}
            data["production_runs"].append(production)
            print(f"{name} production {repeat + 1}/{repeats}: "
                  f"{production['process_qps']:.0f} end-to-end qps, "
                  f"{history['rows']} rows", flush=True)
    runs = data["benchmark_runs"]
    data["median"] = {
        "replay_qps": statistics.median(run["replay_qps"] for run in runs),
        "roundtrip_p50_us": statistics.median(run["roundtrip"]["p50_us"] for run in runs),
        "roundtrip_p99_us": statistics.median(run["roundtrip"]["p99_us"] for run in runs),
        "commit_p99_us": statistics.median(run["input_to_commit"]["p99_us"] for run in runs),
        "production_process_qps": statistics.median(
            run["process_qps"] for run in data["production_runs"]),
    }
    return data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixtures-root", type=Path,
                        default=Path("/private/tmp"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--cases", default=",".join(CASES))
    args = parser.parse_args()
    bench = ROOT / "bazel-bin/hquant/bench/replay_bench"
    server = ROOT / "bazel-bin/hquant/src/hquant_server"
    report = {"host": platform.uname()._asdict(), "configuration": "Bazel --config=release",
              "cases": {}}
    for name in args.cases.split(","):
        workload, events, shards = CASES[name]
        fixture = args.fixtures_root / f"hquant-perf-{name}"
        if not (fixture / "config.yaml").exists():
            write_workload(fixture.resolve(), workload, events, shards)
        report["cases"][name] = run_case(name, fixture, args.repeats, bench, server)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Wrote {args.output}")
