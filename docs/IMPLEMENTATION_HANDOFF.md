# StreamForge 2.0 implementation handoff

Last updated: 2026-10-09

This file is the local continuation guide for moving the project to another
IDE or development session. The authoritative SRS, LLD, and roadmap remain the
[StreamForge master document](https://docs.google.com/document/d/10osXSJlnCJiqLYZv56M18evVEsMtKIcLAq7dVnIAzdQ/edit).
If this handoff and the master document conflict, follow the master document
and update this file in the same change.

## Critical transfer warning

The last committed revision is `3e8cc9d feat: complete StreamForge phases 1-4`.
Phase 5 and this handoff are currently working-tree changes. When moving the
project, copy the entire repository including `.git` and every untracked file,
or create a reviewed commit first. Copying only files known to the last commit
will lose Phase 5.

Before leaving the current IDE:

```powershell
git status --short
git diff --check
```

Generated dependencies, build output, Parquet data, and local test temporary
files are intentionally ignored. Do not transfer `node_modules`, `dist`,
`.tmp`, or large TLC Parquet files unless the destination specifically needs
them.

## Product and correctness contract

StreamForge replays historical NYC TLC Yellow Taxi data. It is not a live taxi
tracking system. Preserve these invariants in every later phase:

1. Event identity is deterministic from the exact source SHA-256 and the
   zero-based logical source-row position.
2. PostgreSQL uniqueness and transaction boundaries are the authoritative
   protection against double counting.
3. Kafka delivery is at least once. Do not claim a distributed exactly-once
   transaction between Kafka and PostgreSQL.
4. `KAFKA_PUBLISHED` means the broker acknowledged publication;
   `DATABASE_COMMITTED` means PostgreSQL committed. Never conflate them.
5. A Kafka offset advances only after the consumer has durably resolved the
   message.
6. Fixed-point distance and fare values remain integers. Event-time analytics
   use UTC pickup-hour buckets.
7. Historical replay, completeness, validation policy, and source provenance
   must remain visible to users.
8. New services own their data and failure lifecycle; they must not directly
   mutate another service's tables.

## Implemented status

### Phase 1 — complete

- Bounded Python/PyArrow Parquet adapter.
- Strict source schema and taxi-zone lookup validation.
- Deterministic source-row event IDs.
- Fixed-point normalization and explicit DST rejection behavior.
- Replay CLI and generated Python gRPC client.
- Current Python suite: 22 tests passing.

### Phase 2 — complete

- Go domain/application boundaries and run-state validation.
- Eight ordered PostgreSQL migrations.
- Atomic accepted-event, aggregate, and run-outcome transaction.
- Retry, cross-run duplicate, concurrency, rejection, migration, and forced
  rollback integration coverage.

### Phase 3 — complete

- Versioned Protobuf/gRPC ingestion contract.
- Bounded requests, stable per-event outcomes, retry/backoff, response-loss
  recovery, and sanitized errors.
- Distinct database and Kafka acknowledgement stages.

### Phase 4 — complete

- Dataset, run, analytics, rejection, and health REST APIs.
- Read-only React/TypeScript historical-replay dashboard.
- Reproducible 100-row demonstration: 100 input, 90 accepted, 10 rejected,
  and 90 aggregate trips.

### Phase 5 — complete but not yet committed

- Official Apache Kafka 4.2.2 single-node KRaft development broker in Compose.
- `streamforge.raw-events.v1`: six partitions and seven-day local retention.
- Stable `event_id` Kafka keys and versioned JSON envelopes.
- Broker acknowledgement before `KAFKA_PUBLISHED` is returned.
- Independent Go analytics consumer with automatic commits disabled.
- Offset commit only after the idempotent PostgreSQL transaction succeeds.
- Durable `kafka_consumer_failures` storage for permanent messages.
- Broker-derived total and per-partition lag REST endpoint and dashboard.
- Automated live crash/restart coverage on both sides of the DB commit.
- Independent 100-message replay group verified no aggregate double count.

Read these records before modifying Phase 5 behavior:

- `docs/adr/006-kafka-at-least-once.md`
- `docs/verification/phase-5-foundation.md`
- `services/core-go/internal/eventstream/`

### Phase 6 — complete

- Spring Boot 4.1.1 service (`services/alerts-java`) on Java 17.
- Defined ADR-007 for independent alerts architecture, event schema compatibility, and deterministic alert ID canonical encoding.
- Table-driven unit-tested RuleEngine (`HIGH_FARE`, `LONG_DISTANCE`, `UNUSUAL_DURATION`).
- Isolated database ownership (`streamforge_alerts`) with Flyway migrations V1 (tables) and V2 (default seed rules).
- Dual idempotency: offset-level tracking via `alert_event_outcomes` and content-level deduplication via SHA-256 `alert_id`.
- Kafka consumer with manual ack mode under consumer group `streamforge-alerts-v1`.
- Complete REST APIs for rules management and alert querying (`/api/v1/alerts-service/rules`, `/alerts`, `/health/live`, `/health/ready`).
- Multi-stage Dockerfile and Compose integration in `deploy/docker/compose.yaml`.
- React dashboard integration with active rules toggling and anomaly alert ledger.
- Full test suite passing (13 tests including Testcontainers PostgreSQL and EmbeddedKafka).
- Phase 6 verification record documented in `docs/verification/phase-6-alerts.md`.

## Current local topology

```text
Python replay CLI
    -> Go core gRPC ingestion
        -> direct PostgreSQL mode, or
        -> Kafka raw-event topic -> Go analytics consumer -> PostgreSQL

Go REST API -> React dashboard

Phase 6:
Kafka raw-event topic -> Java rules consumer -> alerts-owned database/API
```

Default local endpoints:

| Component | Address |
| --- | --- |
| PostgreSQL | `127.0.0.1:5433` |
| Kafka | `127.0.0.1:29092` |
| Go gRPC | `127.0.0.1:50051` |
| Go REST | `127.0.0.1:8080` |
| Java REST | `127.0.0.1:8081` |
| Dashboard | `127.0.0.1:4173` |

Important environment variables:

| Variable | Purpose/default |
| --- | --- |
| `STREAMFORGE_DATABASE_URL` | Required Go PostgreSQL connection URL |
| `STREAMFORGE_GRPC_ADDR` | Core gRPC address; default `127.0.0.1:50051` |
| `STREAMFORGE_HTTP_ADDR` | Core REST address; default `127.0.0.1:8080` |
| `STREAMFORGE_ALERTS_DATABASE_URL` | Java alerts PostgreSQL URL; default `jdbc:postgresql://127.0.0.1:5433/streamforge_alerts` |
| `STREAMFORGE_ALERTS_PORT` | Java alerts HTTP port; default `8081` |
| `STREAMFORGE_KAFKA_BROKERS` | Enables Kafka ingestion when set |
| `STREAMFORGE_KAFKA_TOPIC` | Default `streamforge.raw-events.v1` |
| `STREAMFORGE_KAFKA_CONSUMER_GROUP` | Default `streamforge-analytics-v1` |
| `STREAMFORGE_TEST_DATABASE_URL` | Enables live PostgreSQL integration tests |
| `STREAMFORGE_TEST_KAFKA_BROKERS` | Enables live Kafka crash integration test |
| `VITE_STREAMFORGE_API_BASE` | Optional dashboard API base |

## Restore and verify the development environment

Prerequisites: Docker Desktop, Go 1.24+, Python 3.11+, Node.js/npm, and a JDK
for Phase 6. Run commands from the repository root unless noted.

```powershell
cd deploy\docker
docker compose up -d --wait postgres kafka kafka-init

cd ..\..
python -m pip install -e ".[dev]"
New-Item -ItemType Directory -Force .tmp\pytest | Out-Null
$env:TEMP=(Resolve-Path .tmp\pytest).Path
$env:TMP=$env:TEMP
python -m pytest

cd services\core-go
$env:STREAMFORGE_TEST_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
$env:STREAMFORGE_TEST_KAFKA_BROKERS='127.0.0.1:29092'
go test ./... -count=1
go vet ./...

cd ..\..\apps\dashboard
npm install
npm run build
```

If `.tmp\pytest` does not exist, create it first. The dashboard currently has
no `lint` or automated test script; `npm run build` performs the TypeScript and
Vite production checks.

To run the Kafka services manually, start `go run ./cmd/core` and
`go run ./cmd/analytics` from `services/core-go` with the database and broker
variables set. To exercise the synchronous Phase 1–4 path, omit
`STREAMFORGE_KAFKA_BROKERS` from the core process.

## Remaining roadmap

The master document defines three remaining phases. The task lists and
deliverables below are authoritative summaries; details labeled
"recommended implementation" are proposed sequencing decisions and should be
captured in ADRs when finalized.

## Phase 6 — Java/Spring Boot alerts

Master deliverable: an independent business capability with clear ownership.

Required work:

1. Create `services/alerts-java` as a Spring Boot service with pinned Java,
   Gradle/Maven wrapper, dependency versions, and reproducible tests.
2. Consume `streamforge.raw-events.v1` with an independent consumer group.
   Parse the existing `streamforge.raw-event:v1` envelope without changing Go
   analytics semantics.
3. Implement a rules domain and persistent, versioned rule definitions.
   Thresholds and enabled predicates must be configuration/data, not scattered
   constants.
4. Implement anomaly predicates for structurally valid events. The master
   document identifies unusually high fare, long distance, or unusual duration
   as candidates; exact thresholds are an explicit product decision and must
   not be invented silently.
5. Generate deterministic alert identity from
   `hash(event_id + rule_id + rule_version)` and enforce it with a unique key.
6. Give the service its own persistence ownership for rules, alerts, processing
   outcomes, and permanent consumer failures. It must not write Go aggregate
   tables.
7. Commit Kafka offsets only after the alert transaction or durable no-alert
   outcome commits. A crash after DB commit and before offset commit must replay
   without duplicate alerts.
8. Add alert query and rule-management APIs with bounded pagination, strict
   validation, stable error bodies, and health endpoints.
9. Add unit, database, Kafka integration, duplicate, poison-message, and
   crash/restart tests, including master test `T-K03` for alert idempotency.
10. Extend Compose, README, verification evidence, and the dashboard only after
    the service contract is stable.

Recommended implementation sequence:

1. Write ADR-007 covering the Java service boundary, alert store ownership,
   envelope compatibility, and offset/transaction semantics.
2. Define the rules/alerts schema and deterministic identity test vectors.
3. Build pure rule evaluation with table-driven unit tests.
4. Add repository transactions and live database integration tests.
5. Add the Kafka consumer and crash/replay suite.
6. Add REST APIs, Compose wiring, metrics, and UI integration.

Suggested owned data model, subject to ADR review:

- `alert_rules(rule_id, rule_version, kind, parameters, enabled, created_at)`
- `alerts(alert_id, event_id, rule_id, rule_version, dataset_id, payload,
  created_at)` with a unique deterministic `alert_id`
- `alert_event_outcomes(consumer_group, topic, partition, offset, outcome,
  recorded_at)`
- `alert_consumer_failures(...)` for permanent messages

Phase 6 acceptance evidence:

- Two independent Kafka consumer groups can process the same raw stream.
- Replaying the full topic leaves alert counts unchanged.
- Crash after alert commit but before offset commit creates no duplicate alert.
- Rule versions remain queryable and old alerts retain their rule version.
- Java service failure does not stop Go analytics processing.

## Phase 7 — Kubernetes, Helm, security, and observability

Master deliverable: a repeatable local distributed deployment with documented
recovery.

Required work:

1. Create a local `kind` deployment for the stable Go, Java, dashboard, Kafka,
   and PostgreSQL topology.
2. Add container images, Kubernetes Deployments/Stateful dependencies as
   appropriate, Services, ConfigMaps, Secrets, resource requests/limits, and
   least-privilege service accounts.
3. Create Helm charts with environment-specific values and pinned image tags.
4. Implement correct liveness/readiness/startup probes. Dependency outages may
   make readiness fail but must not cause endless liveness restarts.
5. Add authenticated operator endpoints, role separation for Analyst/Operator/
   Admin, TLS for non-local exposure, network controls, rate limiting, and
   audit-relevant logs. Select the identity/JWT approach in an ADR before
   implementation.
6. Remove development credentials from deployment artifacts. Use secret
   injection and separate least-privilege database identities per service.
7. Add structured metrics and traces for ingestion outcomes, RPC/DB/REST
   latency, producer acknowledgement latency, Kafka lag, active runs, CPU, and
   memory. Preserve request/run correlation without logging raw records.
8. Add CI for generated contracts, Python/Go/Java/frontend checks, migrations,
   image scanning, and Helm/Kubernetes validation.
9. Document and test PostgreSQL backup/restore, Kafka retention/replay limits,
   rolling deployments, pod restart, dependency outage, and recovery.
10. Add rollout and restart evidence; Kubernetes scheduling alone is not
    disaster recovery.

Recommended implementation sequence:

1. Containerize each stable service and add local image smoke tests.
2. Add CI and dependency/container scanning before cluster exposure.
3. Write the security and observability ADRs and threat model draft.
4. Build the kind topology, then package the validated resources with Helm.
5. Add metrics/traces/dashboards and alerting.
6. Execute restart, rollout, database outage, and restore drills.

Phase 7 acceptance evidence:

- A clean machine can create the kind cluster and deploy via documented
  commands.
- Readiness reflects dependencies; liveness reflects process health.
- Secrets do not appear in Git, rendered manifests, images, or logs.
- Go and Java consumers recover from pod restarts without duplicate effects.
- A PostgreSQL backup is restored and reconciled against known aggregates.
- CI validates code, contracts, migrations, images, and Helm output.

## Phase 8 — performance and portfolio release

Master deliverable: an evidence-based engineering portfolio case study.

Required benchmark matrix:

- Dataset sizes: 100,000, then 1,000,000, then 10,000,000 events only when
  measured resources permit.
- Kafka consumer replicas: 1, 2, and 4.
- Fixed, recorded batch sizes, partition counts, seeds, fixture/source SHA-256,
  software versions, machine specifications, and OS.
- Report sustained events/sec; p50/p95/p99 ingestion and query latency; CPU;
  peak RAM/RSS; duplicates; rejections; producer acknowledgement latency;
  consumer lag; and final correctness against an independent Parquet/DuckDB
  baseline.

Targets are hypotheses, not existing results:

- Investigate sustained 500 events/sec initially.
- Target 1,000 events/sec only after measured optimization.
- Evaluate REST p95 below 300 ms only for a precisely defined pre-aggregated
  query workload and documented hardware.

Never convert a target into a claimed result. Record failed or unknown targets
and the bottleneck evidence.

Additional release work:

1. Profile before optimizing; preserve correctness tests through every change.
2. Document before/after measurements and explain batching, partition,
   database, and indexing trade-offs.
3. Create current logical, deployment, data-flow, and failure-sequence
   architecture diagrams.
4. Complete ADRs, operational runbooks, threat model, backup/recovery guide,
   and a one-command or tightly documented demo.
5. Prepare a concise technical interview presentation covering requirements,
   key decisions, failure recovery, measured performance, limitations, and
   next steps.
6. Run the complete release gate: deterministic correctness, cross-language
   contracts, security/recovery checks, known-fixture reconciliation, and
   reproducible benchmark outputs.

## Known gaps intentionally deferred beyond Phase 5

- No Java rules/alerts service or alert storage yet.
- No authentication or RBAC; network interfaces remain loopback-only by
  design.
- No Kubernetes, Helm, container release pipeline, or CI workflow yet.
- No full metrics/tracing stack, SLO dashboard, or operational alerting yet.
- No tested backup/restore runbook yet.
- No recorded 100K/1M/10M benchmark matrix or production-readiness claim.
- No ClickHouse, KEDA, forecasting, cloud deployment, or additional source
  adapters. These remain optional P2 work and require measured justification.

## Immediate next task

Phase 6 (Java/Spring Boot Alerts) and Phase 7 (Kubernetes, Helm, Security Hardening, and Observability) are fully implemented and verified. The immediate next task is **Phase 8 — Performance and Portfolio Release**:

1. Run the benchmark matrix (100K, 1M, 10M events) measuring sustained throughput and p50/p95/p99 latencies across consumer replica scales (1, 2, 4 replicas).
2. Complete end-to-end reconciliation against independent Parquet baselines.
3. Finalize operational runbooks, failure scenario demonstrations, and engineering portfolio documentation.

## Definition of done for all remaining work

A phase is complete only when implementation, automated tests, live integration
evidence, failure semantics, documentation, and reproducible commands agree.
Compiling code or adding a technology to Compose is not sufficient. Preserve
the central proof: replay, retry, timeout, or crash never double-counts a
durably accepted event or alert.
