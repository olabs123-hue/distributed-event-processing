#!/usr/bin/env python3
"""Bounded integration verification: 10k synthetic IDs, replay, worker and DB outages.
Requires Docker Compose; runs only on a fresh disposable stack and destroys its
test volumes on exit. All numbers come from SQL postconditions, not assumptions.
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "benchmarks" / "results" / "docker_integration_latest.json"
TIMEOUT_SECONDS = 300

def docker(*args, timeout=180, quiet=False, extra_env=None):
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    result = subprocess.run(["docker", "compose", *args], cwd=ROOT, env=env,
                            text=True, capture_output=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError("docker compose " + " ".join(args[:3]) +
                           " failed: " + (result.stderr or result.stdout)[-1800:])
    if not quiet and result.stdout.strip():
        print(result.stdout[-800:], flush=True)
    return result.stdout

def count():
    raw = docker("exec","-T","postgres","psql","-U","event_user","-d","events",
                 "-t","-A","-c","SELECT COUNT(*) FROM processed_events;",
                 timeout=20,quiet=True).strip()
    return int(raw)

def wait_until(expected, timeout=TIMEOUT_SECONDS):
    start = time.monotonic()
    while time.monotonic()-start < timeout:
        try:
            current = count()
            if current >= expected:
                return current
        except (RuntimeError, ValueError, subprocess.TimeoutExpired):
            pass
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {expected} persisted unique events")

def produce(amount, start_id):
    docker("run","--rm","-T","-e",f"EVENT_COUNT={amount}","-e",f"START_ID={start_id}",
           "producer",timeout=180,quiet=True)
    print(f"Published synthetic range {start_id}..{start_id+amount-1}",flush=True)

def query(sql):
    return docker("exec","-T","postgres","psql","-U","event_user","-d","events",
                 "-t","-A","-F",",","-c",sql,timeout=30,quiet=True).strip()

def main():
    started=time.monotonic()
    # GitHub-hosted ephemeral runner expected. Do not run against an existing data stack.
    if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("ALLOW_DISPOSABLE_DOCKER_TEST") != "yes":
        raise SystemExit("Run only in GitHub Actions or explicitly opt in with ALLOW_DISPOSABLE_DOCKER_TEST=yes")
    result={"scenario":"Docker local synthetic Kafka/Postgres integration",
            "timestamp_utc":datetime.now(timezone.utc).isoformat(),
            "planned_unique_event_ids":10000,
            "planned_replayed_event_ids":2500,
            "planned_total_producer_messages":12500,
            "kafka_topic_partitions":3,
            "configured_consumer_instances":2,
            "worker_kill_and_restart":False,
            "postgres_outage_and_restart":False}
    try:
        docker("up","--build","-d","kafka","postgres","worker-1","worker-2",timeout=900,quiet=True)
        produce(2500,1)
        wait_until(500)
        docker("kill","-s","KILL","worker-1",timeout=20,quiet=True)
        docker("up","-d","worker-1",timeout=60,quiet=True)
        result["worker_kill_and_restart"]=True
        wait_until(2500)
        produce(2500,1) # All events are duplicates, with the same primary-key IDs.
        time.sleep(4)
        after_replay=count()
        result["unique_rows_after_duplicate_replay"]=after_replay
        if after_replay != 2500:
            raise RuntimeError(f"Expected 2,500 rows after replay; got {after_replay}")
        # Ensure both configured consumers actually persist events, not merely start.
        worker_rows=query("SELECT worker_id,COUNT(*) FROM processed_events GROUP BY worker_id ORDER BY worker_id;")
        active_workers=[line.split(",")[0] for line in worker_rows.splitlines() if line.strip()]
        if len(active_workers) == 1:
            # Halt the worker that took all early partitions, forcing the other member
            # of the consumer group to own the next batch on rebalance.
            active=active_workers[0]
            if active not in ("worker-1","worker-2"):
                raise RuntimeError("Unexpected worker identity stored in DB")
            docker("stop",active,timeout=40,quiet=True)
            produce(500,2501)
            wait_until(3000)
            docker("start",active,timeout=50,quiet=True)
        else:
            produce(500,2501)
            wait_until(3000)
        result["worker_participation_tested"]=True

        docker("stop","postgres",timeout=40,quiet=True)
        produce(7000,3001) # Kafka remains available while Postgres is offline.
        time.sleep(3) # Allow consumers to observe failed persistence attempts.
        # Capture the failure signal while Postgres is offline, before recovery logs
        # can overwhelm the tail.
        observed_logs=docker("logs","--tail","100","worker-1","worker-2",
                             timeout=20,quiet=True)
        result["database_error_observed_in_logs"]="persistence failure" in observed_logs
        docker("start","postgres",timeout=50,quiet=True)
        result["postgres_outage_and_restart"]=True
        wait_until(10000)
        # Ensure no late extra inserts and that all expected event IDs exist.
        time.sleep(3)
        detail=query("SELECT COUNT(*),COUNT(DISTINCT event_id),MIN(event_id),MAX(event_id),"
                     "COUNT(DISTINCT worker_id) FROM processed_events;")
        count_rows,unique,minimum,maximum,workers=map(int,detail.split(","))
        result.update({"persisted_rows":count_rows,"unique_event_ids":unique,
                       "first_event_id":minimum,"last_event_id":maximum,
                       "workers_with_persisted_rows":workers,
                       "duplicate_rows":count_rows-unique,
                       "missing_unique_ids":10000-unique,
                       "verified_sql_id_range":minimum==1 and maximum==10000,
                       "integration_wall_seconds":round(time.monotonic()-started,2)})
        if (count_rows,unique,minimum,maximum)!=(10000,10000,1,10000):
            raise RuntimeError("PostgreSQL final-state reconciliation failed")
        if workers < 2:
            raise RuntimeError("Only one worker persisted rows despite forced takeover test")
        if not result["database_error_observed_in_logs"]:
            raise RuntimeError("No observed database failure in worker logs during induced outage")
        result["passed"]=True
    except Exception as e:
        result["passed"]=False
        result["error"]=str(e)
        try:
            logs=docker("logs","--tail","40","worker-1","worker-2",quiet=True,timeout=20)
            print(logs[-2000:],flush=True)
        except Exception:
            pass
        raise
    finally:
        OUT.parent.mkdir(parents=True,exist_ok=True)
        OUT.write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")
        print("INTEGRATION_RESULT "+json.dumps(result,sort_keys=True),flush=True)
        try:
            docker("down","-v","--remove-orphans",timeout=120,quiet=True)
        except Exception as e:
            print("Cleanup warning:", str(e),flush=True)

if __name__=="__main__":
    main()
