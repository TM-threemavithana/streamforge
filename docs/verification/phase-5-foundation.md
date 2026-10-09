# Phase 5 Kafka verification record

Verified locally on 2026-10-09. The Phase 5 asynchronous pipeline, failure
semantics, replay safety, and consumer-lag observability are complete.

## Topology and contract

- Apache Kafka 4.2.2 runs in single-node KRaft mode through Docker Compose.
- `streamforge.raw-events.v1` has six partitions, replication factor one for
  local development, and seven-day retention.
- Stable source-derived `event_id` values are Kafka message keys.
- The ingestion service waits for all-ISR broker acknowledgment and returns
  `KAFKA_PUBLISHED`; this is distinct from `DATABASE_COMMITTED`.
- `streamforge-analytics-v1` disables automatic commits and advances each
  offset only after PostgreSQL durably resolves the event.
- Unsupported or permanently invalid records are persisted in
  `kafka_consumer_failures` before their offsets advance.

## Executed evidence

- Go 1.24 unit and package tests passed, including event-envelope routing,
  permanent/transient failure behavior, and acknowledgment-stage mapping.
- `go vet ./...` passed.
- All Go tests passed against live PostgreSQL after applying eight forward
  migrations.
- A deterministic 100-row fixture was published through Python -> gRPC ->
  Kafka. Broker-side partition totals reconciled to 100 records.
- A fresh analytics group consumed the retained records to lag zero and the run
  completed with 100 input, 90 accepted, 10 rejected, and 90 aggregate trips.
- A second independent group replayed all 100 records from the beginning. All
  six partitions again reached lag zero; PostgreSQL remained at 90 aggregate
  trips and 10 rejection rows, proving full redelivery did not double-count.
- The automated live crash/restart test created an isolated Kafka topic, then
  simulated termination before database processing and after database commit
  but before offset commit. Both records were redelivered; PostgreSQL finished
  with exactly two events, two run outcomes, and two aggregate trips.
- `GET /api/v1/operations/kafka-lag` was verified against the live broker. It
  reported all six partitions from committed and end offsets, with total lag
  zero for the caught-up verification group.
- The React dashboard displays total and per-partition lag. A lag-read failure
  does not prevent durable PostgreSQL analytics from loading.

## Boundary of this phase

Reproducible throughput/load benchmarking remains part of the later planned
performance evidence stage. Phase 5 establishes measurable Kafka lag and
correct behavior at both consumer crash boundaries; it does not claim a
distributed exactly-once transaction across Kafka and PostgreSQL.
