#!/usr/bin/env python3
"""Measure actual Kafka -> C++ worker -> PostgreSQL persistence.

Each worker-count experiment uses its own unique Docker Compose project and
volume. Neither the user's default stack nor their existing database is touched.
The event timestamp is created by the C++ producer immediately before sending.
PostgreSQL records its processing timestamp on successful insertion.
Latency = processed_at_ms - created_at_ms (milliseconds).
"""
from __future__ import annotations
import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OVERRIDE = ROOT / "benchmarks" / "docker-compose.benchmark.yml"
OUTPUT = ROOT / "benchmarks" / "results" / "kafka_postgres_e2e_latest.json"

def run(args: list[str], *, timeout: int = 180) -> str:
    proc = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if proc.returncode:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(args[:9])}\n"
                           + (proc.stderr or proc.stdout)[-3000:])
    return proc.stdout.strip()

def docker(project: str, *args: str, timeout: int = 180) -> str:
    return run(["docker", "compose", "-p", project, "-f", "docker-compose.yml",
                "-f", str(OVERRIDE), *args], timeout=timeout)

def sql(project: str, statement: str) -> str:
    return docker(project, "exec", "-T", "postgres", "psql", "-X", "-v",
                  "ON_ERROR_STOP=1", "-U", "event_user", "-d", "events",
                  "-t", "-A", "-F", ",", "-c", statement, timeout=30).strip()

def wait_for_rows(project: str, target: int, timeout: int) -> int:
    deadline = time.monotonic() + timeout
    last = 0
    while time.monotonic() < deadline:
        try:
            last = int(sql(project, "SELECT count(*) FROM processed_events;"))
            if last >= target:
                return last
        except (RuntimeError, ValueError, subprocess.TimeoutExpired):
            pass
        time.sleep(0.5)
    raise TimeoutError(f"Timed out: only {last}/{target} distinct events persisted")

def scenario(workers: int, count: int, timeout: int, *, preserve: bool = False) -> dict:
    project = f"atlaskve2e{workers}{uuid.uuid4().hex[:7]}"
    started_at = datetime.now(timezone.utc).isoformat()
    result = {"workers_requested": workers, "events": count, "compose_project": project,
              "timestamp_utc": started_at, "success": False}
    try:
        print(f"Starting isolated {workers}-consumer stack ({project})", flush=True)
        docker(project, "up", "--build", "-d", "--scale",
               f"worker-bench={workers}", "worker-bench", timeout=900)
        # Make sure table creation is complete before injecting the first events.
        wait_for_rows(project, 0, 90)
        topic = docker(project, "exec", "-T", "kafka", "rpk", "topic", "describe",
                       "market-events", "--brokers", "kafka:9092", timeout=25)
        print("TOPIC_METADATA " + topic[-3500:], flush=True)
        # rpk describe normally shows a per-partition table with PARTITION column.
        # Explicit partition routing is unsafe if the broker has a different count.
        topic_lines = topic.splitlines()
        observed_partitions = set()
        for line in topic_lines:
            fields = line.split()
            if fields and fields[0].isdigit():
                observed_partitions.add(int(fields[0]))
        if observed_partitions != {0, 1, 2}:
            raise RuntimeError("Expected three available Kafka partitions 0,1,2; "
                               + f"metadata rows show {sorted(observed_partitions)}")
        # Allow consumer-group members to subscribe and reach a stable assignment.
        # Otherwise a short batch may finish before the second consumer joins.
        time.sleep(12)
        # This measures the overall publish+drain interval, not producer-only rate.
        begin = time.monotonic()
        docker(project, "run", "--rm", "-T", "-e", f"EVENT_COUNT={count}",
               "-e", "START_ID=1", "-e", "BENCHMARK_PARTITIONS=3", "producer", timeout=max(180, timeout))
        producer_done = time.monotonic()
        try:
            wait_for_rows(project, count, timeout)
        except TimeoutError:
            print("WORKER_ROW_COUNTS " + sql(project,
                "SELECT worker_id,COUNT(*) FROM processed_events GROUP BY worker_id ORDER BY worker_id;"), flush=True)
            print("WORKER_LOGS " + docker(project, "logs", "--tail", "50",
                "worker-bench", timeout=30)[-5500:], flush=True)
            raise
        end = time.monotonic()
        raw = sql(project, """
            SELECT COUNT(*),
                   COUNT(DISTINCT event_id),
                   MIN(event_id), MAX(event_id),
                   COUNT(DISTINCT worker_id),
                   COUNT(*) FILTER (WHERE processed_at_ms < created_at_ms),
                   ROUND(AVG(processed_at_ms - created_at_ms)::numeric, 2),
                   ROUND(percentile_cont(0.50) WITHIN GROUP
                       (ORDER BY processed_at_ms-created_at_ms)::numeric, 2),
                   ROUND(percentile_cont(0.95) WITHIN GROUP
                       (ORDER BY processed_at_ms-created_at_ms)::numeric, 2),
                   ROUND(percentile_cont(0.99) WITHIN GROUP
                       (ORDER BY processed_at_ms-created_at_ms)::numeric, 2)
            FROM processed_events
        """)
        parts = raw.split(",")
        if len(parts) != 10:
            raise RuntimeError(f"Unexpected SQL result: {raw[:500]}")
        rows, unique_ids, min_id, max_id, active_consumers, negative_latencies = map(int, parts[:6])
        if (rows, unique_ids, min_id, max_id) != (count, count, 1, count):
            raise RuntimeError(f"Nonmatching DB totals: {parts[:4]}")
        if active_consumers < min(workers, 3):
            # Include useful diagnostics rather than silently claiming scaling.
            print("WORKER_ROWS " + sql(project, "SELECT worker_id,COUNT(*) FROM processed_events GROUP BY worker_id ORDER BY worker_id;"), flush=True)
            print("CONSUMER_LOGS " + docker(project, "logs", "--tail", "40", "worker-bench", timeout=25)[-2500:], flush=True)
            raise RuntimeError(f"Only {active_consumers} distinct worker IDs persisted records; expected at least {min(workers, 3)}")
        if negative_latencies:
            raise RuntimeError("Detected negative timestamp differences (clock issue)")
        elapsed = end - begin
        if elapsed <= 0:
            raise RuntimeError("Non-positive measurement interval")
        result.update({
            "success": True,
            "unique_rows_persisted": unique_ids,
            "distinct_worker_ids_with_rows": active_consumers,
            "kafka_partitions": 3,
            "producer_seconds_including_compose_run_overhead": round(producer_done - begin, 3),
            "producer_plus_database_drain_seconds": round(elapsed, 3),
            "end_to_end_persisted_events_per_second": round(count / elapsed, 2),
            "mean_event_age_at_insert_ms": float(parts[6]),
            "p50_event_age_at_insert_ms": float(parts[7]),
            "p95_event_age_at_insert_ms": float(parts[8]),
            "p99_event_age_at_insert_ms": float(parts[9]),
            "latency_method": "PostgreSQL processed_at_ms minus producer created_at_ms",
            "throughput_method": "all unique rows / wall time from docker compose producer run to SQL target reached",
            "notes": ("Local synthetic Kafka-compatible Redpanda and PostgreSQL; "
                      "single host clocks; per-scenario fresh compose volume. "
                      "4 consumers share 3 partitions, so one may be idle. "
                      "Not production throughput or an SLA.")
        })
        print("SCENARIO_RESULT " + json.dumps(result, sort_keys=True), flush=True)
        return result
    finally:
        if not preserve:
            try:
                docker(project, "down", "-v", "--remove-orphans", timeout=150)
                print(f"Cleaned isolated project {project}", flush=True)
            except Exception as exc:
                print(f"WARNING: cleanup failed for {project}: {exc}", file=sys.stderr)

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--events", type=int, default=3000)
    p.add_argument("--workers", nargs="+", type=int, default=[1, 2, 4])
    p.add_argument("--timeout", type=int, default=300)
    p.add_argument("--output", type=Path, default=OUTPUT)
    p.add_argument("--preserve-stacks", action="store_true",
                   help="DEBUG ONLY: leave isolated stack running; consumes resources")
    args = p.parse_args()
    if not 100 <= args.events <= 100000:
        p.error("--events must be in [100,100000]")
    if not args.workers or any(w not in [1, 2, 4] for w in args.workers):
        p.error("--workers must be a nonempty subset of 1 2 4")
    if len(set(args.workers)) != len(args.workers):
        p.error("worker counts must be distinct")
    if not 30 <= args.timeout <= 1800:
        p.error("--timeout must be 30..1800 seconds")
    if not OVERRIDE.exists():
        p.error(f"Missing benchmark compose override: {OVERRIDE}")
    if not os.getenv("ALLOW_KAFKA_BENCHMARK") == "yes":
        p.error("Explicit opt-in required: set ALLOW_KAFKA_BENCHMARK=yes (isolated disposable Docker stacks)")
    run(["docker", "compose", "version"], timeout=20)
    results = []
    try:
        for workers in args.workers:
            results.append(scenario(workers, args.events, args.timeout,
                                    preserve=args.preserve_stacks))
    finally:
        payload = {
            "benchmark": "Kafka -> C++ workers -> PostgreSQL",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "completed_scenarios": len(results),
            "expected_scenarios": len(args.workers),
            "all_succeeded": len(results) == len(args.workers) and all(r["success"] for r in results),
            "results": results,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        csv_path = args.output.with_suffix(".csv")
        if results:
            with csv_path.open("w", encoding="utf-8", newline="") as fh:
                columns = ["workers_requested", "events", "distinct_worker_ids_with_rows",
                           "end_to_end_persisted_events_per_second", "p50_event_age_at_insert_ms",
                           "p95_event_age_at_insert_ms", "p99_event_age_at_insert_ms",
                           "producer_plus_database_drain_seconds"]
                writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(results)
        print("BENCHMARK_SUMMARY " + json.dumps(payload, sort_keys=True), flush=True)

if __name__ == "__main__":
    main()
