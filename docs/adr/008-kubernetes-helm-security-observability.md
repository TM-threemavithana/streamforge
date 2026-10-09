# ADR-008: Kubernetes Deployment Topology, Security Architecture, and Observability

Status: accepted for Phase 7

## Context

StreamForge has evolved into a distributed polyglot system composed of:
1. `core-go`: gRPC ingestion service and REST query/catalog API.
2. `analytics-worker`: Go Kafka stream processor computing pre-aggregated analytics.
3. `alerts-java`: Spring Boot 4 service evaluating real-time anomaly rules against raw Kafka events.
4. `dashboard`: React/TypeScript single-page application.
5. `postgres`: Relational data store partitioned across two distinct service ownership domains (`streamforge` and `streamforge_alerts`).
6. `kafka`: Event broker handling `streamforge.raw-events.v1` and consumer groups.

Phase 7 requires packaging this topology into repeatable, production-grade container images and Kubernetes manifests managed by Helm, hardening container and network security, establishing structured observability, and providing automated backup and recovery procedures.

## Decisions

### 1. Deployment Topology & Packaging

- **Packaging Unit**: A unified Helm chart located at `deploy/helm/streamforge`.
- **Target Environments**:
  - `local`: Local Kubernetes cluster (e.g., kind, k3s, Docker Desktop Kubernetes) for rapid reproducible testing and integration drills.
  - `prod`: Hardened configuration with externalized secrets, scaled replicas, and restricted ingress.
- **Service Topology**:
  - `streamforge-core`: Deployment exposing port 8080 (REST) and port 50051 (gRPC).
  - `streamforge-analytics`: Deployment (1+ replicas) running the Kafka event stream consumer.
  - `streamforge-alerts`: Deployment exposing port 8081 (REST) and consuming Kafka alerts.
  - `streamforge-dashboard`: Deployment running Nginx reverse proxy and static asset server on port 8080.
  - `streamforge-postgres`: StatefulSet (or deployment with PVC) hosting dedicated databases: `streamforge` (owned by `streamforge_user`) and `streamforge_alerts` (owned by `streamforge_alerts_user`).
  - `streamforge-kafka`: Single-node KRaft broker for local/kind deployments with persistent storage.

### 2. Container Security and Least Privilege

All application container images enforce defense-in-depth:
- **Non-Root Execution**: Containers run under dedicated non-root service user `streamforge` (`UID 10001`, `GID 10001`). No container executes as UID 0.
- **Immutable Root Filesystem**: Deployments declare `readOnlyRootFilesystem: true`. Temporary scratch directories (e.g., `/tmp`, `/var/cache/nginx`) use explicit in-memory `emptyDir` mounts.
- **Privilege Escalation**: `allowPrivilegeEscalation: false` is strictly enforced.
- **Linux Capabilities**: All Linux capabilities are dropped: `capabilities: drop: ["ALL"]`.
- **Service Account Hardening**: Service accounts set `automountServiceAccountToken: false` to prevent pod credential exfiltration unless explicitly required.
- **Base Images**: Minimal attack surface utilizing `alpine:3.21` (Go), `eclipse-temurin:17-jre-alpine` (Java), and `nginxinc/nginx-unprivileged:alpine` (Dashboard).

### 3. Secret Management & Identity Isolation

- **Zero Credentials in Git/Helm Values**: Development credentials must never be committed to Git or embedded in default Helm template values. Secrets are injected via Kubernetes `Secret` resources (`streamforge-secrets`, `streamforge-db-credentials`).
- **Database Identity Separation**:
  - `core-go` and `analytics-worker` connect strictly using user `streamforge_user` with grants limited to the `streamforge` database.
  - `alerts-java` connects strictly using user `streamforge_alerts_user` with grants limited to the `streamforge_alerts` database. Neither service possesses administrative rights or cross-database access.

### 4. Health Probe Contracts: Liveness vs Readiness vs Startup

To prevent cascading restarts during downstream dependency degradation:
- **Liveness Probes (`/health/live`)**:
  - Validates process liveliness only (event loop responding, HTTP server accepting connections).
  - Downstream dependency failures (e.g., transient database partition or Kafka rebalance) MUST NOT cause liveness probe failures.
  - Frequency: 10s period, 3 failures before restart.
- **Readiness Probes (`/health/ready`)**:
  - Validates downstream connectivity (PostgreSQL ping, Kafka broker check).
  - If a dependency fails, the pod is removed from Kubernetes Service endpoints to prevent traffic routing, but process memory and caches remain intact.
  - Frequency: 5s period, 1 failure to mark unready, 1 success to restore.
- **Startup Probes**:
  - Configured for `alerts-java` to accommodate JVM and Flyway migration startup latency (e.g., up to 60s failure threshold) without premature liveness SIGKILL.

### 5. Access Control & Operator Roles

Role separation follows three tiers:
1. **Analyst**: Read-only queries to aggregate analytics (`/api/v1/analytics/*`), runs, datasets, and alerts ledger.
2. **Operator**: Run lifecycle control (`/complete`, `/cancel`), rule enablement toggling (`PATCH /rules/{id}/.../status`), and Kafka lag inspection.
3. **Admin**: Dataset registration, rule schema authoring, disaster recovery procedures, and infrastructure maintenance.

For cluster ingress, an authorization proxy or API gateway terminates TLS, authenticates JWT claims containing `roles: ["analyst" | "operator" | "admin"]`, and injects validated role headers into internal service requests.

### 6. Observability & Tracing Architecture

- **Prometheus Metrics Scraping**:
  - Services expose standard Prometheus text exposition endpoints (`/metrics` for Go, `/actuator/prometheus` for Java).
  - Pod annotations enable automatic scraper discovery:
    `prometheus.io/scrape: "true"`
    `prometheus.io/port: "<port>"`
    `prometheus.io/path: "<metrics-path>"`
- **Request Correlation**:
  - All ingress requests receive or propagate an `X-Request-ID` header (128-bit hex or UUID).
  - Ingress, downstream RPCs, and Kafka headers carry this identifier to enable end-to-end tracing across language boundaries without logging raw customer data.

### 7. Backup and Disaster Recovery (DR)

- **PostgreSQL Recovery Point Objective (RPO) and Recovery Time Objective (RTO)**:
  - RPO: Maximum 1 hour (periodic automated `pg_dump` backups).
  - RTO: Under 5 minutes for automated restore.
- **Kafka Replay Buffer**:
  - Kafka retention is configured to 7 days.
  - In the event of consumer failure or corrupted aggregate tables, consumer offsets can be reset to any historical timestamp within the retention window, deterministically repopulating analytics and alerts via idempotent deduplication keys.
- **Automated Verification**:
  - Backup scripts dump schema and data into verified, compressed archives.
  - Restoration drill script tests point-in-time restore against a temporary database and verifies aggregate invariants.

## Consequences

- Full local deployment can be instantiated with Helm into any standard Kubernetes or kind environment.
- Microservices cannot compromise host filesystems or read unauthorized sibling databases.
- Pods tolerate transient dependency network blips without triggering fatal crash loops.
- Deterministic event IDs and offset replays ensure zero duplicate analytics or alert records during pod failure and recovery drills.
