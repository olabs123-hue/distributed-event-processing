# Distributed Real-Time Event Processing Platform
> **Portfolio highlight — distributed systems:** Explore Kafka partitioned consumption, C++ workers, idempotent PostgreSQL writes, API access, and failure-recovery testing. This project showcases backend and systems depth rather than a React user interface; consult the benchmark section before quoting any throughput.


A distributed event-processing platform built around C++, Apache Kafka, PostgreSQL, Docker, Python, and AWS infrastructure. The system is designed to demonstrate concurrency, message-driven architecture, fault tolerance, idempotent processing, observability, benchmarking, and horizontal scaling.

## What the system does

```text
                    +------------------+
                    | C++ Event        |
                    | Producer         |
                    +--------+---------+
                             |
                             v
                    +------------------+
                    | Apache Kafka     |
                    | 3 partitions     |
                    +--------+---------+
                             |
                  +----------+----------+
                  |                     |
                  v                     v
          +---------------+     +---------------+
          | C++ Worker 1  |     | C++ Worker 2  |
          +-------+-------+     +-------+-------+
                  |                     |
                  +----------+----------+
                             v
                    +------------------+
                    | PostgreSQL       |
                    | Idempotent store |
                    +--------+---------+
                             |
                             v
                    +------------------+
                    | Python REST API  |
                    +------------------+
```

Kafka consumers use the same consumer group so partitions can be processed by multiple workers. Database writes happen before Kafka offsets are committed. If a worker fails after a database write but before the offset commit, Kafka can redeliver the event and the PostgreSQL primary key prevents duplicate insertion.

## Engineering goals

- C++17 systems programming
- Kafka producer/consumer architecture
- Multiple worker instances
- At-least-once delivery semantics
- Idempotent database writes
- PostgreSQL persistence
- Retry/failure handling
- Real throughput and latency measurements
- Dockerized local deployment
- CI build/test pipeline
- AWS infrastructure foundation

## Repository structure

```text
src/
  common/       Event model and JSON wire format
  kafka/        Kafka producer/consumer wrapper
  producer/     C++ event producer
  worker/       C++ Kafka worker
  db/           PostgreSQL persistence layer
  api/          Python FastAPI service
python/
  benchmark/    End-to-end Kafka/PostgreSQL benchmark
benchmarks/
  results/      Generated benchmark output
  docs/         Architecture notes
  docker/       Container definitions
tests/          C++ tests
infra/aws/     Terraform AWS foundation
.github/        CI workflow
```

## Run locally with Docker

Requirements:

- Docker Desktop / Docker Engine with Compose
- At least 4 GB RAM available to Docker

Start the stack:

```bash
docker compose up --build
```

The stack contains:

- Kafka
- PostgreSQL
- two C++ workers
- one C++ producer
- Python API

Check the API:

```text
http://localhost:8000/health
http://localhost:8000/stats
```

The producer currently sends 100 events when the Compose stack starts.

## Run the benchmark

Install the benchmark dependencies:

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# Linux/macOS: source .venv/bin/activate
pip install -r python/benchmark/requirements.txt
```

Make sure Kafka and PostgreSQL are running, then run:

```bash
python python/benchmark/benchmark.py
```

Override the workload if needed:

```bash
EVENT_COUNT=10000 python python/benchmark/benchmark.py
```

The benchmark reports measured values for:

- producer throughput
- average end-to-end processing latency
- p50 latency
- p95 latency
- p99 latency

No benchmark number is claimed in this README until it has actually been measured.

## Measured benchmark result

The environment used for this benchmark did not have Docker, Kafka, or PostgreSQL available, so no Kafka-to-PostgreSQL end-to-end throughput number is claimed here. Instead, the repository includes a reproducible C++ event serialization benchmark that was executed locally.

Test configuration:

- 1,000,000 iterations per run
- 129-byte JSON payload
- C++17, `g++ -O3 -DNDEBUG`
- 3 benchmark runs
- Linux x86_64 environment with 3 visible CPUs

Measured results:

| Run | Serialization ops/s | Round-trip ops/s |
|---:|---:|---:|
| 1 | 1,674,943 | 1,292,106 |
| 2 | 1,746,354 | 1,320,478 |
| 3 | 1,630,234 | 1,314,470 |

Across the three runs, the arithmetic mean was approximately **1.68 million serializations/s** and **1.31 million serialize/deserialize round trips/s**. These are measurements of the event wire-format implementation only; they are **not** representative of Kafka, PostgreSQL, or end-to-end distributed-system throughput.

The raw results are stored in `benchmarks/results/event_serialization_benchmark_2026-09-22.csv`.

## Fault-tolerance design

The worker disables Kafka auto-commit.

Processing flow:

```text
Kafka consume
     |
     v
Parse event
     |
     v
PostgreSQL transaction
     |
     +---- failure ---> retry / redelivery
     |
     v
Commit Kafka offset
```

If the database transaction succeeds but the Kafka offset commit fails, the event can be delivered again. `event_id` is the primary key, so the second database insert becomes a no-op. This gives the system an explicit at-least-once + idempotent processing model.

## CI/CD

GitHub Actions currently:

1. Installs C++ dependencies.
2. Configures the CMake project.
3. Builds the producer, worker, and tests.
4. Runs CTest.
5. Builds the Docker application image.

The next CI/CD stage can publish a tested image to ECR and deploy the service to ECS/Fargate.

## AWS roadmap

Terraform currently creates an ECR repository and CloudWatch log group as the starting point for cloud deployment.

Planned AWS architecture:

```text
                 Internet
                    |
              Load Balancer
                    |
              Python API
                    |
       +------------+------------+
       |                         |
     ECS                       RDS
   Workers                  PostgreSQL
       |
      MSK
     Kafka
```

Planned production additions:

- VPC/private subnets
- ECS/Fargate
- Amazon MSK
- RDS PostgreSQL
- Secrets Manager
- IAM least-privilege roles
- CloudWatch metrics and alarms
- autoscaling
- deployment rollback

AWS resources should not be created blindly because cloud services can incur charges.

## Testing

Build and test locally:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

## Performance methodology

Benchmarking will be based on actual runs rather than estimated numbers. Important dimensions include:

- event count
- producer throughput
- end-to-end latency
- p50/p95/p99 latency
- number of workers
- Kafka partition count
- CPU utilization
- memory utilization
- database write throughput
- failure/retry behavior

Scaling experiments should compare 1, 2, and 3+ workers against the same workload and record the results in `benchmarks/results/`.


## Verified multi-worker Kafka → PostgreSQL scaling benchmark (25 November 2026)

**[Successful three-scenario GitHub Actions benchmark](https://github.com/olabs123-hue/distributed-event-processing/actions/runs/45601157117)** · [Benchmark script](benchmarks/kafka_postgres_e2e.py) · [Reproduction instructions](benchmarks/END_TO_END.md)

The benchmark now **explicitly creates and verifies the three-partition topic before starting consumers**. Each scenario uses its own disposable Redpanda + PostgreSQL Compose stack, publishes 5,000 synthetic unique IDs, and verifies PostgreSQL persisted the entire ID range. Crucially, the SQL postconditions also verify that the expected number of different workers actually inserted rows.

| Configured consumers | Distinct DB-writing workers | Unique persisted events | Batch throughput (events/s) | p50 event age | p99 event age |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 5,000 | **1,025.30** | 1,694 ms | 2,976 ms |
| 2 | 2 | 5,000 | **1,151.79** | 1,126 ms | 2,256.01 ms |
| 4 | 3 | 5,000 | **1,574.81** | 896 ms | 1,438 ms |

In this single run, the 4-configured-consumer / 3-active-writer configuration delivered **~1.54× the measured batch throughput** of the single-consumer baseline and ~52% lower p99 event age. **Only three workers can actively own partitions on this three-partition topic.** Do not report the outcome as four-way active scaling.

**Measurement boundaries:** synthetic containers on a shared GitHub Actions runner, a single measured batch per worker count, producer command overhead included in throughput timing, event age calculated from producer `created_at_ms` to database `processed_at_ms`, and no production SLA or external exchange traffic. The p50/p99 values are database-insert event-age statistics, **not** client request acknowledgment latencies. Repeat trials and confidence intervals are still needed to establish stable scaling behavior.

Previous runs below are retained transparently: they suffered incomplete topic initialization and did not demonstrate multiple active writers. The new run above supersedes them for the **scaling** claim.

## End-to-end Kafka to PostgreSQL measurements (25 November 2026)

[Successful 1,000-event benchmark run](https://github.com/olabs123-hue/distributed-event-processing/actions/runs/45597410081) · [Benchmark methodology](benchmarks/END_TO_END.md)

| Configured C++ consumers | Synthetic unique events | Persisted batch throughput | p50 event age | p99 event age | Workers that actually persisted |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,000 | 376.88 events/s | 859 ms | 1,117 ms | 1 |
| 2 | 1,000 | 377.73 events/s | 816.5 ms | 1,064 ms | 1 |
| 4 | 1,000 | 372.23 events/s | 814 ms | 1,067 ms | 1 |

Measurements are from disposable Redpanda/Kafka-compatible and PostgreSQL containers in a GitHub Actions runner. **All three scenarios persisted 1,000 unique rows, but only one worker contributed database writes in each case. These are valid measured batch drain timings, not evidence of horizontal scaling.** The later benchmark configuration increases the batch to 5,000 events, waits for consumer-group assignment, and requires multi-worker contribution before reporting the scaling test as successful. The p50/p99 values are timestamp-derived age at database insert, not request/acknowledgment latency.

### Larger 5,000-event batch (25 November 2026)

The [successful measured 5,000-event batch run](https://github.com/olabs123-hue/distributed-event-processing/actions/runs/45597784618) reported:

| Configured consumers | Unique events persisted | Batch drain throughput | p50 event age | p99 event age | Actual database-writing workers |
|---:|---:|---:|---:|---:|---:|
| 1 | 5,000 | 622.17 events/s | 3,359.5 ms | 6,088.01 ms | 1 |
| 2 | 5,000 | 578.70 events/s | 3,718.5 ms | 6,513.01 ms | 1 |
| 4 | 5,000 | 577.86 events/s | 3,709.5 ms | 6,518.01 ms | 1 |

This run **did not demonstrate scaling**: only a single unique worker ID appeared in persisted data in every case, despite more workers being configured. The recorded `success` flag verified persisted row totals, **not multi-worker participation**. A subsequent benchmark version includes a hard assertion that all expected workers (capped by Kafka partition count) actually persist rows. Until that passes, do not claim throughput gains from scaling.

### Post-fix scaling verification status (25 November 2026)

**Passing post-fix CI evidence is available:** [run 45601157117](https://github.com/olabs123-hue/distributed-event-processing/actions/runs/45601157117) completed all three tested configurations. Its raw CI logs confirm **1, 2, and 3 distinct PostgreSQL-writing workers** with **1, 2, and 4 configured consumers**, respectively, persisting 5,000 unique records per scenario. With 3 Kafka partitions, the fourth configured consumer is not an additional active partition writer.

The same-run single-consumer batch throughput was **1,025.30 events/s**, versus **1,574.81 events/s** for **4 configured / 3 active workers** (approximately **53.6% higher**). That is stronger evidence of scaling than the older 622.17 events/s test, which used different benchmark code and logging behavior. The new three-worker configuration comparison should use the same-run 1,025.30 baseline rather than claiming an apples-to-apples improvement against the older 622.17 figure.

The benchmark workflow has subsequently been extended to test **1, 2, 3, and 4 configured consumers** and produce a specific three-worker scaling verdict. **That newer four-scenario workflow has not yet been independently verified as passing here.** Repeat the synthetic experiment to assess stability across runs; a single shared-runner CI result is not production capacity or an SLA.

[Benchmark workflow](.github/workflows/e2e-benchmark.yml) · [Acceptance and measurement methodology](benchmarks/END_TO_END.md)

## Verified 10,000-event Docker integration and recovery (18 November 2026)

[GitHub Actions successful integration run](https://github.com/olabs123-hue/distributed-event-processing/actions/runs/44302567380) · [Raw JSON result artifact](https://github.com/olabs123-hue/distributed-event-processing/actions/runs/44302567380/artifacts/14425654070) · [Reproducible test methodology](docs/INTEGRATION_TEST.md)

A disposable Docker test used a **three-partition Kafka-compatible Redpanda topic**, **two C++ consumer-group workers**, and PostgreSQL. It published **12,500 synthetic market-data messages** comprising **10,000 distinct event IDs** and **2,500 deliberately replayed IDs**. The test killed and restarted one worker, forced a second worker to take over processing, stopped and restarted PostgreSQL, and observed a worker database failure during the outage.

Verified final SQL results:

- **10,000 unique persisted rows** covering the complete ID range **1–10,000**
- **0 missing IDs** and **0 duplicate database rows** after the 2,500-ID replay
- **2 workers with persisted rows**, confirming actual participation by both consumer-group instances
- Database error observed in worker logs; final reconciliation succeeded after PostgreSQL restarted
- A measured **204.28-second integration workflow phase** (includes Docker image startup, worker handover, outages, restarts and waiting for catch-up). **This is not end-to-end throughput.**

This is a synthetic Docker integration experiment on a shared GitHub-hosted runner, **not** live exchange ingestion, a production resilience guarantee, or 12,500 unique events. This system uses at-least-once Kafka processing plus idempotent PostgreSQL writes, not exactly-once Kafka delivery.

### CV-safe summary

> Tested two C++ Kafka consumers and PostgreSQL against 12,500 synthetic messages (10,000 unique IDs plus 2,500 replays); after worker and database disruptions, verified 10,000 unique persisted rows, zero missing IDs, zero duplicate rows, and writes from both workers.

Only the separate **1.68 million/sec** metric below refers to standalone C++ serialization; do not report it as Kafka-to-PostgreSQL throughput.

## Author

**Olamide Olabosoye**  
Computer Engineering, University of Lagos

GitHub: https://github.com/olabs123-hue
