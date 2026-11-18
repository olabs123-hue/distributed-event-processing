# Synthetic Kafka/PostgreSQL integration test

The GitHub Actions [Docker integration workflow](../.github/workflows/integration.yml) runs a **disposable** stack and checks final PostgreSQL contents. The test sends 2,500 synthetic market event IDs, kills and restarts one worker, replays the same 2,500 IDs, then stops PostgreSQL while producing 7,500 more unique events. After restarting PostgreSQL, it waits until the database contains 10,000 unique IDs in the range 1–10,000 and verifies no duplicate rows. Two Kafka consumers share the `event-workers` group, with a three-partition topic.

The C++ worker retries the same message if PostgreSQL fails, and commits Kafka offsets only after a successful insert or a recognized duplicate. The test can catch both lost rows and duplicate database rows. **The producer emits 12,500 messages; only 10,000 are unique IDs.**

Results are only established after a workflow run succeeds. Check the `distributed-event-integration-results` GitHub Actions artifact for actual rows, timing, worker participation and failure checks. These are *not* production events or exchange data. Wall-clock integration runtime includes startup and recovery; do not label it Kafka processing throughput.

Locally, this test destroys its own Docker volumes and therefore requires explicit opt-in:
```bash
ALLOW_DISPOSABLE_DOCKER_TEST=yes python3 tests/integration_docker.py
```
Avoid running this test against a valued local Docker Compose stack. Default port exposure is limited to loopback for the optional API profile.
