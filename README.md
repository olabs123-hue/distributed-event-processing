# Distributed Real-Time Event Processing Platform

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

## Author

**Olamide Olabosoye**  
Computer Engineering, University of Lagos

GitHub: https://github.com/olabs123-hue
