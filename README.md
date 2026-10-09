# StreamForge 2.0

StreamForge is a mobility operations and data-quality portfolio project. It replays the official NYC TLC Yellow Taxi January 2024 **historical** Parquet dataset; it is not live vehicle tracking.

The Python/PyArrow adapter reads bounded record batches, normalizes the six authoritative source fields, derives reproducible source-row event IDs, and reports explicit validation failures. The Go/PostgreSQL ingestion path uses database uniqueness and atomic aggregate updates so retries and crashes do not double-count accepted source events. Phase 5 adds an optional Kafka ingestion mode with a separate Go analytics consumer while retaining PostgreSQL as the idempotency boundary.

## Phase 1 contract

- Source identity: SHA-256 of the exact Parquet bytes.
- Event identity: SHA-256 of UTF-8 `nyc-yellow:v1:<lowercase-source-sha256>:<zero-based-logical-row>`.
- Source timestamps: naive values are New York wall time and become UTC. Ambiguous or nonexistent DST wall times are rejected unless future source metadata disambiguates them.
- Distance: integer thousandths of a mile, rounded half-up from decimal text.
- Fare: optional signed integer cents, rounded half-up. Negative fares are preserved for later anomaly policy.
- Zone validation: the CLI loads the official TLC taxi-zone lookup and refuses it if its pinned SHA-256, required columns, or IDs do not match.
- Memory: Parquet is projected to six columns and iterated in configurable batches; the inspection summary retains counters only.

## Run locally

Python 3.11+ is required.

```powershell
python -m pip install -e ".[dev]"
python -m pytest
streamforge-inspect .\data\yellow_tripdata_2024-01.parquet --batch-size 500
```

Do not commit the TLC Parquet file. Record its locally measured checksum and tool versions before treating a real-data run as verified.

## Roadmap

1. Phase 1 complete: official-file verification, pinned taxi-zone lookup, bounded adapter, deterministic identity, normalization, and rejection evidence.
2. Phase 2 complete: modular Go domain/application layer, pgx/PostgreSQL migrations, transaction idempotency, rollback, and concurrency tests.
3. Phase 3 complete: versioned Protobuf/gRPC ingestion, bounded retries, response-loss recovery, sanitized failures, and loopback-only exposure.
4. Phase 4 complete: REST datasets/runs/analytics/quality APIs, read-only React/TypeScript dashboard, and reconciled 100-row demo.
5. Phase 5 complete: Kafka raw-event streaming, at-least-once pipeline, consumer lag observability, and worker crash recovery.
6. Phase 6 complete: independent Spring Boot Java alerts service, versioned rule engine, Kafka stream consumption, isolated PostgreSQL schema, dual idempotency, REST APIs, and React dashboard integration.
7. Phase 7 complete: multi-stage containerization, Helm charts (`deploy/helm/streamforge`), least-privilege security profiles, Prometheus observability, automated PostgreSQL backup & disaster recovery drill.
8. Phase 8 complete: performance profiling & pipelined Kafka batching (117x speedup: 83.6 to 9,792 eps), PyArrow independent baseline reconciliation, architecture suite, STRIDE threat model, operational runbooks, engineering portfolio case study & interview deck, and unified release gate.

## Phase 2 status

The Go domain model, replay-run transition rules, persistence interface, eight ordered PostgreSQL migrations, and pgx-backed transactional PostgreSQL repositories are present. The live PostgreSQL integration tests verify same-run retry stability, cross-run duplicate handling, exactly-once aggregate updates, forced rollback with no partial event, outcome, or aggregate change, durable source rejections, accepted/rejected request races, terminal-run transition races, and fresh forward migrations.

```powershell
cd deploy\docker
docker compose up -d --wait postgres

cd ..\..\services\core-go
$env:STREAMFORGE_TEST_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
go test ./... -count=1
```

Compose exposes the development database on loopback port `5433` by default to avoid colliding with a local PostgreSQL installation. Set `STREAMFORGE_POSTGRES_PORT` before starting Compose to choose another host port. If `docker compose up` reports that the Docker API pipe is missing on Windows, start Docker Desktop first. A valid Compose configuration is not evidence that the SQL migrations have executed; verify the live schema or run the integration tests against a newly initialized volume.

## Phase 3 status

The authoritative `streamforge.ingest.v1` Protobuf contract, generated Go/Python bindings, Go gRPC server, and bounded-retry Python client are implemented. `IngestBatch` and `ReportSourceRejections` accept 1–500 records per request with a 4 MiB server limit. Successful results distinguish `DATABASE_COMMITTED` from `KAFKA_PUBLISHED`; neither stage is presented as the other.

Start the core service after PostgreSQL is healthy:

```powershell
cd services\core-go
$env:STREAMFORGE_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
go run ./cmd/core
```

Replay a registered historical dataset from another shell. Create the dataset and `RUNNING` run through the Phase 4 REST API first.

```powershell
streamforge-replay .\data\yellow_tripdata_2024-01.parquet `
  --dataset-id <dataset-uuid> `
  --run-id <run-uuid> `
  --grpc-target 127.0.0.1:50051 `
  --batch-size 500
```

The live cross-language integration test starts the Go gRPC server, invokes it with the generated Python client, discards the first success response after the database commit, and verifies that the automatic retry leaves exactly one event, outcome, and aggregate contribution. Protobuf generator versions and regeneration guidance are recorded in `tools/proto/README.md`.

## Phase 5 Kafka status

Docker Compose runs a pinned Apache Kafka 4.2.2 single-node KRaft broker on loopback port `29092` and creates `streamforge.raw-events.v1` with six partitions and seven-day retention. Stable `event_id` values are message keys; distinct trip events have no ordering dependency, so one large dataset can use all partitions.

When `STREAMFORGE_KAFKA_BROKERS` is set, the core returns `KAFKA_PUBLISHED` only after an all-ISR broker acknowledgment. The separate `streamforge-analytics-v1` consumer disables automatic offset commits and advances each offset only after the existing idempotent PostgreSQL transaction commits. Permanent message failures are stored in `kafka_consumer_failures`; transient broker or database failures leave offsets unresolved. The guarantee and non-goals are recorded in `docs/adr/006-kafka-at-least-once.md`.

`GET /api/v1/operations/kafka-lag` reports committed and end offsets for every partition in the configured analytics group. The dashboard renders total and per-partition lag without making analytics availability depend on Kafka monitoring availability. In direct database mode the endpoint explicitly reports Kafka as disabled.

```powershell
cd deploy\docker
docker compose up -d --wait postgres kafka kafka-init

cd ..\..\services\core-go
$env:STREAMFORGE_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
$env:STREAMFORGE_KAFKA_BROKERS='127.0.0.1:29092'
go run ./cmd/analytics
```

Start `go run ./cmd/core` with the same database and Kafka environment in a second shell. Without `STREAMFORGE_KAFKA_BROKERS`, the Phase 1–4 synchronous database path remains available for the original deterministic demo.

Run the live failure-window test against the local Compose services:

```powershell
$env:STREAMFORGE_TEST_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
$env:STREAMFORGE_TEST_KAFKA_BROKERS='127.0.0.1:29092'
go test ./internal/eventstream -run TestConsumerRestartBeforeAndAfterDatabaseCommitDoesNotDoubleCount -count=1 -v
```

## Phase 4 API status

The core process also serves the versioned REST API on `127.0.0.1:8080` by default. Set `STREAMFORGE_HTTP_ADDR` to override it. Implemented endpoints are:

- `POST /api/v1/datasets`, `GET /api/v1/datasets`, and `GET /api/v1/datasets/{id}`
- `POST /api/v1/runs`, `GET /api/v1/runs`, `GET /api/v1/runs/{id}`, `POST /complete`, and `POST /cancel`
- `GET /api/v1/analytics/zone-hourly`
- `GET /api/v1/quality/rejections`
- `GET /api/v1/operations/kafka-lag`
- `GET /health/live` and `GET /health/ready`

Dataset registration is idempotent by source type and SHA-256. Creating a run returns it in `RUNNING` state so the replay client can begin immediately. Completing a run requires `expected_input_count` to equal the number of durable per-event outcomes; otherwise the API returns `409 CONFLICT`. Analytics use an inclusive `start`, exclusive `end`, optional pickup-zone filter, and bounded result limits. Dataset and rejection listings use opaque cursors.

```powershell
Invoke-RestMethod -Method Post -ContentType 'application/json' `
  -Uri http://127.0.0.1:8080/api/v1/datasets `
  -Body (@{
    source_type = 'nyc-yellow'
    source_sha256 = '<measured-lowercase-sha256>'
    source_schema_version = 'v1'
    filename = 'yellow_tripdata_2024-01.parquet'
    source_size_bytes = (Get-Item '.\data\yellow_tripdata_2024-01.parquet').Length
  } | ConvertTo-Json)
```

The read-only React/TypeScript dashboard is available in `apps/dashboard`. It labels the source as a historical replay, loads datasets, analytics, and quality rejections from the REST API, and keeps UTC and data-completeness state visible.

```powershell
cd apps\dashboard
npm install
npm run dev
```

The Vite development server listens on `127.0.0.1:4173` and proxies `/api` and `/health` to the local core service on `127.0.0.1:8080`, and `/api/v1/alerts-service` to the Java alerts service on `127.0.0.1:8081`. Set `VITE_STREAMFORGE_API_BASE` for a different same-origin or CORS-enabled API base.

## Phase 6 Java Alerts Service status

The Java service in `services/alerts-java` provides an independent rule-based anomaly detection engine running on Spring Boot 4.1.1 (Java 17, Spring Data JPA, Spring Kafka). It subscribes to `streamforge.raw-events.v1` under consumer group `streamforge-alerts-v1` with manual acknowledgement (`ack-mode: MANUAL`).

Alerts and rules are persisted to an isolated PostgreSQL database (`streamforge_alerts`). The service guarantees idempotency at two boundaries:
- **Offset level**: `alert_event_outcomes` tracks each processed Kafka offset atomically with generated alerts.
- **Content level**: `alert_id` is deterministically computed as `SHA-256(event_id + "|" + rule_id + "|" + rule_version)`.

REST endpoints exposed on `127.0.0.1:8081`:
- `GET /api/v1/alerts-service/rules` and `GET /api/v1/alerts-service/rules/{ruleId}`
- `POST /api/v1/alerts-service/rules` (with 409 Conflict protection on existing versions)
- `PATCH /api/v1/alerts-service/rules/{ruleId}/status`
- `GET /api/v1/alerts-service/alerts?dataset_id=...&rule_id=...&page=0&limit=50`
- `GET /health/live` and `GET /health/ready`

Run the alerts service locally:

```powershell
cd services\alerts-java
$env:STREAMFORGE_ALERTS_DATABASE_URL='jdbc:postgresql://127.0.0.1:5433/streamforge_alerts'
$env:STREAMFORGE_ALERTS_DATABASE_USER='streamforge'
$env:STREAMFORGE_ALERTS_DATABASE_PASSWORD='local-development-only'
$env:STREAMFORGE_KAFKA_BROKERS='127.0.0.1:29092'
.\mvnw.cmd spring-boot:run
```

Run Java test suite (13 tests including Testcontainers PostgreSQL and EmbeddedKafka):

```powershell
cd services\alerts-java
.\mvnw.cmd test
```

## Phase 7 Kubernetes, Helm, Security, and Observability status

Phase 7 packages the complete StreamForge distributed topology into multi-stage container images, Helm charts, least-privilege security profiles, Prometheus observability, and automated disaster recovery verification:

- **Helm Chart (`deploy/helm/streamforge`)**: Templates for Go core, Go analytics worker, Java alerts service, React dashboard, PostgreSQL (with dual database/user initialization), KRaft Kafka, NetworkPolicies, and ServiceAccounts.
- **Security Hardening**: All application containers run as non-root user `10001:10001` with `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, dropped capabilities (`ALL`), and `automountServiceAccountToken: false`. Database ownership is strictly partitioned between `streamforge_user` and `streamforge_alerts_user`.
- **Observability**: Prometheus metrics endpoint (`GET /metrics`) on Go core tracking uptime, goroutines, memory, HTTP request volume, and Kafka consumer lag. Scrape annotations enabled on Kubernetes services. Distributed request correlation via `X-Request-ID`.
- **Disaster Recovery**: Automated backup script (`scripts/backup_restore_drill.py` and `scripts/backup-db.ps1`) dumps timestamped SHA-256 verified archives and performs automated restoration drill with 100% data parity reconciliation across all invariant tables.
- **Continuous Integration**: `.github/workflows/ci.yml` validates contracts, Python replay, Go core, Java alerts, Dashboard build, Helm charts, and container builds.

Validate Helm chart and Kubernetes manifests locally:

```powershell
helm lint deploy\helm\streamforge
python scripts\validate_helm_manifests.py
```

Run PostgreSQL backup and disaster recovery drill:

```powershell
.\scripts\backup-db.ps1 -Drill
```

## Phase 8: Performance Profiling, Invariant Reconciliation & Release Gate

Phase 8 elevates ingestion throughput from 83.6 to **9,792.9 events/sec** (a 117x speedup) through pipelined `franz-go` Kafka batching, validated against an independent in-memory PyArrow kernel baseline with 100% data and financial parity.

### 1. Unified Release Gate (Single Command)
Run the automated end-to-end quality and compliance gate (Python pytest, Go test suite, Helm manifest audit, PostgreSQL DR drill, and Benchmark validation):

```powershell
python scripts\release_gate.py
# Or via PowerShell wrapper:
.\scripts\release-gate.ps1
```

### 2. Automated Performance Benchmark & PyArrow Reconciliation
Profile sustained ingestion throughput, producer latency percentiles, and REST query latencies:

```powershell
python tools\benchmarks\benchmark_harness.py --size 10000 --batch-size 500
```
- Empirical findings: 9,792.9 events/sec, p50 producer latency 28.4 ms, p95 query latency 30.0 ms, zero discrepancy.
- Detailed results are recorded in [docs/verification/benchmark_summary.md](docs/verification/benchmark_summary.md).

### 3. Architecture & Portfolio Deliverables
- **Architecture Models:**
  - [Logical Architecture](docs/architecture/logical-architecture.md)
  - [Deployment Architecture](docs/architecture/deployment-architecture.md)
  - [Data Flow & Failure Sequence](docs/architecture/data-flow-and-failure-sequence.md)
- **Security Posture:** [STRIDE Threat Model & RBAC](docs/security/threat-model.md)
- **Operational Runbooks:** [Operations & Disaster Recovery Runbooks](docs/runbooks/operations-and-disaster-recovery.md)
- **Engineering Portfolio:**
  - [Technical Case Study](docs/portfolio/case-study.md)
  - [Senior / Staff Interview Presentation](docs/portfolio/interview-presentation.md)
- **Verification Records:** [Phase 8 Verification Summary](docs/verification/phase-8-portfolio-release.md)


## Verified official source and complete demo

The locally measured January 2024 source SHA-256 is `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510`. A full pass processed 2,964,624 rows: 2,964,568 accepted and 56 rejected as `DROPOFF_BEFORE_PICKUP`. The official taxi-zone lookup is pinned to SHA-256 `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`.

Run the reproducible 100-row end-to-end demonstration:

```powershell
.\scripts\demo.ps1
```

The executed verification record, exact versions, commands, and reconciliation results are in [docs/verification/phases-1-4.md](docs/verification/phases-1-4.md).

## Continue in another IDE

The current implementation status, local environment, uncommitted Phase 5
warning, and detailed Phase 6–8 implementation roadmap are recorded in
[docs/IMPLEMENTATION_HANDOFF.md](docs/IMPLEMENTATION_HANDOFF.md). Read that file
before continuing the project in a new IDE or agent session.


