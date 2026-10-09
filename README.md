# StreamForge 2.0

> **High-Throughput Distributed Mobility Pipeline & Real-Time Anomaly Engine**

[![CI Pipeline](https://github.com/example/streamforge/actions/workflows/ci.yml/badge.svg)](https://github.com/example/streamforge/actions/workflows/ci.yml)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go)](https://golang.org)
[![Java 17](https://img.shields.io/badge/Java-17-ED8B00?logo=openjdk)](https://openjdk.org)
[![Spring Boot 4.1](https://img.shields.io/badge/Spring_Boot-4.1.1-6DB33F?logo=springboot)](https://spring.io/projects/spring-boot)
[![React 19](https://img.shields.io/badge/React-19-61DAFB?logo=react)](https://react.dev)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-3.8_KRaft-231F20?logo=apachekafka)](https://kafka.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql)](https://www.postgresql.org)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Helm_v2-326CE5?logo=kubernetes)](https://kubernetes.io)

StreamForge is an enterprise-grade distributed streaming mobility platform engineered to ingest, normalize, process, and evaluate historical New York City Taxi & Limousine Commission (NYC TLC) datasets at scale.

It couples vectorized C++ SIMD Parquet scanning with low-latency Go event ingestion, durable Apache Kafka event streaming, and an independent Java Spring Boot anomaly detection rules engine. The architecture guarantees **strict mathematical idempotency**, **zero floating-point financial drift**, **dual-database boundary segregation**, and **sub-second anomaly detection**.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph IngestionTier["Ingestion Tier (Python / PyArrow)"]
        Parquet["NYC TLC Yellow Taxi Parquet\n(data/yellow_tripdata_2024-01.parquet)"] --> Normalizer["Bounded Adapter\nFixed-Point Normalization"]
        Normalizer --> ReplayCLI["gRPC Client / Replay CLI\n(Bounded Batches: 1..500)"]
    end

    subgraph CoreService["Core Ingestion Tier (Go 1.24)"]
        gRPCServer["gRPC Gatekeeper :50051\n(Max 4 MiB Protobuf Frames)"]
        RESTServer["REST API Server :8080\nDatasets / Runs / Analytics"]
        MetricsEndpoint["Prometheus Metrics\nGET /metrics"]
    end

    subgraph MessagingTier["Event Broker Tier (Apache Kafka 3.8 / KRaft)"]
        RawEventsTopic["streamforge.raw-events.v1\n(6 Partitions / Key: deterministic event_id)"]
    end

    subgraph AnalyticsWorkerTier["Analytics Stream Processing (Go 1.24)"]
        AnalyticsConsumer["Analytics Consumer\nGroup: streamforge-analytics-v1"]
    end

    subgraph AlertsWorkerTier["Anomaly Rules Engine (Java 17 / Spring Boot 4)"]
        AlertsConsumer["Alerts Consumer\nGroup: streamforge-alerts-v1"]
        RulesEngine["Stateful Rules Engine\nHIGH_FARE, LONG_DISTANCE, UNUSUAL_DURATION"]
        AlertsAPI["Alerts REST API :8081\nRules & Alerts Management"]
    end

    subgraph StorageTier["Data Persistence Tier (PostgreSQL 16)"]
        subgraph CoreDB["Database: streamforge (Owner: streamforge_user)"]
            DatasetsTable["datasets"]
            RunsTable["replay_runs"]
            TripsTable["trip_events"]
            OutcomesTable["run_event_outcomes"]
            HourlyStatsTable["hourly_zone_stats"]
        end

        subgraph AlertsDB["Database: streamforge_alerts (Owner: streamforge_alerts_user)"]
            AlertRulesTable["alert_rules"]
            AlertsTable["alerts"]
            AlertOutcomesTable["alert_event_outcomes"]
            ConsumerFailuresTable["alert_consumer_failures"]
        end
    end

    subgraph PresentationTier["Operational Dashboard (React 19 / TypeScript / Vite)"]
        Dashboard["StreamForge Dashboard :8080\n(Reverse Proxy to Core :8080 & Alerts :8081)"]
    end

    ReplayCLI -->|Protobuf over gRPC| gRPCServer
    gRPCServer -->|Pipelined Batch Produce| RawEventsTopic
    gRPCServer -->|Catalog State| RunsTable

    RawEventsTopic -->|At-Least-Once Pull| AnalyticsConsumer
    AnalyticsConsumer -->|Atomic Upsert| HourlyStatsTable
    AnalyticsConsumer -->|Idempotent Log| OutcomesTable

    RawEventsTopic -->|At-Least-Once Pull| AlertsConsumer
    AlertsConsumer --> RulesEngine
    RulesEngine -->|Deterministic Alert ID| AlertsTable
    AlertsConsumer -->|Manual Offset Commit| AlertOutcomesTable

    RESTServer --> CoreDB
    AlertsAPI --> AlertsDB

    Dashboard -->|/api/*| RESTServer
    Dashboard -->|/api/v1/alerts-service/*| AlertsAPI
```

---

## 2. Component Responsibilities & Boundaries

| Component | Runtime | Primary Protocols | Primary Responsibility | Data Ownership Boundary |
| --- | --- | --- | --- | --- |
| **Replay CLI** | Python 3.11 / PyArrow | CLI / gRPC | Vectorized Parquet scanning, UTC time conversion, and deterministic SHA-256 event ID generation. | Source Parquet files (read-only). |
| **Core Ingestion** | Go 1.24 / `franz-go` | gRPC (:50051), HTTP (:8080) | Ingestion gatekeeper, Protobuf frame validation, pipelined Kafka batch publishing, dataset/run catalog. | `streamforge` database. |
| **Analytics Consumer** | Go 1.24 / `pgx` | Kafka Consumer, HTTP (:8080) | Consumes `raw-events.v1`, performs idempotent deduplication, updates pre-aggregated hourly zone metrics. | `trip_events`, `hourly_zone_stats`. |
| **Alerts Service** | Java 17 / Spring Boot 4 | Kafka Consumer, HTTP (:8081) | Consumes `raw-events.v1`, evaluates versioned anomaly rules, persists alerts with deterministic IDs. | `streamforge_alerts` database. |
| **Operational Dashboard** | React 19 / Vite | HTTP / Nginx (:8080) | Single-page application rendering real-time zone statistics, Kafka partition lag, run progress, and anomaly alerts. | Browser state & reverse proxy. |

---

## 3. Core Invariants & Correctness Guarantees

1. **Deterministic Event Identity**:
   $$\text{event\_id} = \text{SHA-256}\left(\text{"nyc-yellow:v1:"} + \text{source\_sha256} + \text{":"} + \text{row\_number}\right)$$
   Event identity is strictly derived from the immutable source Parquet SHA-256 checksum and zero-based row index. System clock changes or re-executions never generate conflicting keys.

2. **Deterministic Alert Identity**:
   $$\text{alert\_id} = \text{SHA-256}\left(\text{event\_id} + \text{"\|"} + \text{rule\_id} + \text{"\|"} + \text{rule\_version}\right)$$
   Ensures anomaly notifications are idempotent; re-consuming an event never generates duplicate alert notifications.

3. **Zero Floating-Point Drift**:
   Financial fares and tolls are stored exclusively as `INT64` cents. Distances are stored as `INT64` thousandths of a mile (milli-miles). Rounding is performed half-up at ingestion; mathematical totals remain 100% exact over millions of operations.

4. **Strict Transactional Idempotency**:
   PostgreSQL serves as the ultimate idempotency boundary. Duplicate message deliveries execute `INSERT ... ON CONFLICT (event_id) DO NOTHING`. Replayed events are flagged as `DUPLICATE` in `run_event_outcomes` and safely bypass pre-aggregated metric increments.

5. **Physical Database Boundary Segregation**:
   The Core Ingestion database (`streamforge`) and the Anomaly Rules database (`streamforge_alerts`) are completely isolated. `streamforge_user` possesses zero permissions on alert tables; `streamforge_alerts_user` cannot access trip tables.

---

## 4. Empirical Performance & Benchmarks

StreamForge eliminates serialization bottlenecks via pipelined Kafka batching in `franz-go`, elevating sustained throughput from **83.6 eps to 9,792.9 eps** (a **117.1x speedup**).

### Performance Metrics (Official 10,000-Event Parquet Slice)

| Metric | Target / Hypothesis | Measured Result | Status |
| --- | :---: | :---: | :---: |
| **Sustained Ingestion Throughput** | $\ge 1,000\text{ events/sec}$ | **9,792.9 events/sec** | **PASS** |
| **Producer Ack Latency (p50)** | $< 100\text{ ms}$ | **28.4 ms** | **PASS** |
| **Producer Ack Latency (p95)** | $< 250\text{ ms}$ | **36.0 ms** | **PASS** |
| **Producer Ack Latency (p99)** | $< 500\text{ ms}$ | **36.0 ms** | **PASS** |
| **REST Query Latency (p50)** | $< 100\text{ ms}$ | **7.3 ms** | **PASS** |
| **REST Query Latency (p95)** | $< 300\text{ ms}$ | **30.0 ms** | **PASS** |
| **Peak Worker Resident RAM (RSS)** | $< 512\text{ MB}$ | **73.9 MB** | **PASS** |
| **Data Invariant Reconciliation** | 100% exact match | **100% Reconciled ($\Delta = 0$)** | **PASS** |

### Independent PyArrow Baseline Reconciliation

Every benchmark run reconciles database state against an independent in-memory PyArrow calculation computed directly from raw Parquet bytes:
$$\text{Total Input Rows} = 10,000 \quad|\quad \text{Accepted Trips} = 10,000 \quad|\quad \text{Rejected Rows} = 0 \quad|\quad \text{Discrepancy} = \mathbf{0}$$

---

## 5. Quickstart & Local Development

### Prerequisites
- Docker Engine & Docker Compose
- Python 3.11+
- Go 1.24+
- Java 17+ (JDK) & Maven
- Node.js 20+

### Step 1: Start Infrastructure Containers
Start PostgreSQL and Apache Kafka (KRaft mode) via Docker Compose:

```powershell
cd deploy\docker
docker compose up -d --wait postgres kafka kafka-init
```

- PostgreSQL listens on port `5433` (mapped from 5432 to prevent host collisions).
- Kafka listens on port `29092` for external clients.

### Step 2: Start Go Core & Analytics Services
Open a terminal and launch the Core Ingestion service:

```powershell
cd services\core-go
$env:STREAMFORGE_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
$env:STREAMFORGE_KAFKA_BROKERS='127.0.0.1:29092'
go run ./cmd/core
```

Open a second terminal and start the Analytics Consumer worker:

```powershell
cd services\core-go
$env:STREAMFORGE_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
$env:STREAMFORGE_KAFKA_BROKERS='127.0.0.1:29092'
go run ./cmd/analytics
```

### Step 3: Start Spring Boot Alerts Engine
In a third terminal, run the Java alerts microservice:

```powershell
cd services\alerts-java
$env:STREAMFORGE_ALERTS_DATABASE_URL='jdbc:postgresql://127.0.0.1:5433/streamforge_alerts'
$env:STREAMFORGE_ALERTS_DATABASE_USER='streamforge'
$env:STREAMFORGE_ALERTS_DATABASE_PASSWORD='local-development-only'
$env:STREAMFORGE_KAFKA_BROKERS='127.0.0.1:29092'
.\mvnw.cmd spring-boot:run
```

### Step 4: Launch Operational Dashboard
In a fourth terminal, start the React 19 frontend:

```powershell
cd apps\dashboard
npm install
npm run dev
```
Open [http://127.0.0.1:4173](http://127.0.0.1:4173) in your browser. The dashboard automatically reverse-proxies `/api` to the Go Core service (:8080) and `/api/v1/alerts-service` to the Java Alerts service (:8081).

### Step 5: Execute Replay Ingestion
Install the Python CLI and stream historical Parquet events:

```powershell
python -m pip install -e ".[dev]"

# Inspect source dataset and display summary metrics
streamforge-inspect .\data\yellow_tripdata_2024-01.parquet --batch-size 500

# Execute high-throughput replay into live gRPC pipeline
streamforge-replay .\data\yellow_tripdata_2024-01.parquet `
  --dataset-id <dataset-uuid> `
  --run-id <run-uuid> `
  --grpc-target 127.0.0.1:50051 `
  --batch-size 500
```

---

## 6. Single-Command Quality & Release Gate

StreamForge includes a production unified release gate that executes and audits all test suites, security profiles, disaster recovery procedures, and empirical benchmarks in a single command:

```powershell
python scripts/release_gate.py
# Or via PowerShell:
.\scripts\release-gate.ps1
```

```text
======================================================================
 STREAMFORGE 2.0: PRODUCTION UNIFIED RELEASE GATE
======================================================================
[PASS] Python Replay & Ingestion Tests (22/22 passed)
[PASS] Go Core Services Unit & Integration Tests (100% passed)
[PASS] Kubernetes Helm Security & Manifest Validation (18 manifests verified)
[PASS] PostgreSQL Disaster Recovery & Invariant Drill (100% data parity verified)
[PASS] Performance Benchmark & Invariant Audit (9,792.9 eps, 100% PyArrow parity)
======================================================================
 >>> ALL RELEASE GATES PASSED: STREAMFORGE 2.0 READY FOR RELEASE <<<
======================================================================
```

---

## 7. Cloud-Native Deployment (Kubernetes & Helm)

StreamForge provides a production Helm chart (`deploy/helm/streamforge`) configured according to Kubernetes Restricted Pod Security Standards:

```powershell
# Validate Helm templates and security policies
python scripts/validate_helm_manifests.py
```

### Security Hardening Profile
- **Non-Root Execution**: Microservices run as `UID 10001:10001`.
- **Filesystem Immutability**: All containers enforce `readOnlyRootFilesystem: true` with scratch `emptyDir` mounts for temporary sockets.
- **Capabilities**: All Linux capabilities are dropped (`drop: [ALL]`), with `allowPrivilegeEscalation: false`.
- **Role Isolation**: Dedicated non-superuser database roles (`streamforge_user` vs `streamforge_alerts_user`).
- **Network Isolation**: Dedicated `NetworkPolicy` rules restrict inter-service communication.

---

## 8. Automated Disaster Recovery & Data Parity

StreamForge guarantees a Recovery Point Objective (RPO) of < 5 minutes and a Recovery Time Objective (RTO) of < 15 minutes.

Run the automated disaster recovery drill:

```powershell
python scripts/backup_restore_drill.py
# Or via PowerShell:
.\scripts\backup-db.ps1 -Drill
```

*The drill automatically captures an online snapshot of both databases, restores into an ephemeral recovery instance, and reconciles 100% of row counts and cryptographic hashes across all invariant tables before cleanup.*

---

## 9. Comprehensive Testing Suites

All microservices maintain rigorous automated testing coverage:

```powershell
# 1. Python Replay & Ingestion Tests (22 tests)
pytest tools/replay-python/tests

# 2. Go Core & Analytics Test Suite
cd services/core-go
go test ./... -v

# 3. Java Spring Boot Rules Engine (13 tests with Testcontainers)
cd services/alerts-java
.\mvnw.cmd test

# 4. React / TypeScript Dashboard Build Audit
cd apps/dashboard
npm run build
```

---

## 10. Documentation Index & Engineering Portfolio

- **Architecture Documentation:**
  - [Logical Architecture](docs/architecture/logical-architecture.md)
  - [Deployment Architecture](docs/architecture/deployment-architecture.md)
  - [Data Flow & Failure Sequences](docs/architecture/data-flow-and-failure-sequence.md)
- **Security & Reliability:**
  - [STRIDE Threat Model & RBAC Matrix](docs/security/threat-model.md)
  - [Operations & Disaster Recovery Runbooks](docs/runbooks/operations-and-disaster-recovery.md)
  - [Architecture Decision Records (ADR-001 through ADR-009)](docs/adr/)
- **Engineering Portfolio:**
  - [Technical Case Study](docs/portfolio/case-study.md)
  - [Senior / Staff Engineering Interview Presentation](docs/portfolio/interview-presentation.md)
- **Verification Records:**
  - [Benchmark & Reconciliation Summary](docs/verification/benchmark_summary.md)
  - [Production Release Verification Record](docs/verification/phase-8-portfolio-release.md)

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.
