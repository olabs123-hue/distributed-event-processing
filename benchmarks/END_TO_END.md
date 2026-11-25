# Kafka → PostgreSQL end-to-end benchmark
This benchmark runs **fresh isolated Docker Compose projects** and measures synthetic producer-created events persisted by the C++ workers into PostgreSQL. It is entirely separate from the serialization microbenchmark and recovery integration test.

## Quick start

Requirements: Docker Engine/Desktop with Compose V2, Python 3.9+, and enough local disk and memory for Kafka-compatible Redpanda + PostgreSQL + 4 workers. **Do not run on a valuable Docker project:** although the script uses unique project names and volumes, it creates and removes its own disposable test containers and volumes.

### Linux/macOS
```bash
export ALLOW_KAFKA_BENCHMARK=yes
python3 benchmarks/kafka_postgres_e2e.py --events 3000 --workers 1 2 4
```

### Windows PowerShell
```powershell
$env:ALLOW_KAFKA_BENCHMARK = "yes"
py benchmarks/kafka_postgres_e2e.py --events 3000 --workers 1 2 4
```

**You do not need to run `docker compose up` first.** The script starts new ephemeral Compose stacks itself. It uses the three-partition `market-events` topic and a new PostgreSQL volume for each worker-count scenario. Worker counts are requested consumer instances; with only **3 Kafka partitions**, scaling from three to four consumers might not increase throughput.

The script produces events through the repository's C++ producer, records wall-clock time from beginning the producer command until all unique event IDs appear in PostgreSQL, and then calculates:

- **Throughput (events/s):** successfully persisted unique rows divided by producer-plus-database-drain wall time, including container command overhead. This is an end-to-end batch drain throughput, not maximum steady-state throughput.
- **Latency (ms), mean/p50/p95/p99:** PostgreSQL `processed_at_ms` minus the C++ producer's `created_at_ms` for each event; includes Kafka wait and worker/DB delay. It is **event age at database insert**, not an independently measured acknowledgment latency or Kafka offset-commit latency.
- Unique-row count, observed distinct worker IDs with persisted rows, and full ID-range verification.

`benchmarks/results/kafka_postgres_e2e_latest.json` and sibling `.csv` are produced **only after an actual run**. Do not claim a result before inspecting those artifacts and checking `all_succeeded: true`. The first run may include Docker image builds and pulls; repeat runs under consistent conditions before comparing.

On GitHub Actions the dedicated [benchmark workflow](../.github/workflows/e2e-benchmark.yml) can be dispatched manually. Neither the script nor CI modifies your existing default Compose volume.

## Important limitations
This is a **synthetic local Docker benchmark** using Redpanda's Kafka-compatible API, not a paid cloud Kafka cluster, production exchange feed, or published service-level objective. Client and server containers run on one Docker host. Producer timestamps use millisecond resolution; the p99 is a database record-age percentile over the batch, not a client round-trip percentile. A four-worker test with three Kafka partitions can leave at least one consumer without a partition.
