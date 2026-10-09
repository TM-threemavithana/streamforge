# ADR-009: Performance Benchmarking, Scaling Matrix, and Portfolio Release

Status: accepted for Phase 8

## Context

Phase 8 is the final phase of StreamForge, synthesizing all previous milestones into an evidence-based engineering case study and portfolio release. Prior phases built:
- Deterministic event normalization and Parquet batch replay (Phase 1)
- Transactional persistence and idempotent aggregate updates (Phase 2)
- Strict Protobuf/gRPC ingestion with bounded retries (Phase 3)
- Multi-dimensional analytics queries and real-time dashboard (Phase 4)
- At-least-once Kafka raw event streaming with consumer crash recovery (Phase 5)
- Independent Spring Boot Java anomaly alerting service with dual idempotency (Phase 6)
- Multi-stage containerization, Helm packaging, security hardening, and disaster recovery (Phase 7)

To prove production readiness and support technical portfolio review, Phase 8 establishes an empirical benchmark harness, tests scaling hypotheses, reconciles results against an independent PyArrow baseline, and completes operational documentation and architecture diagrams.

## Decisions

### 1. Benchmark Matrix & Hypotheses

We test performance using explicit hypotheses rather than unsubstantiated claims:
- **Workload Sizes**:
  - `10,000 events`: Fast smoke test and correctness gate.
  - `100,000 events`: Standard baseline benchmark across all configurations.
  - `1,000,000 events`: Sustained high-throughput stress test.
- **Consumer Concurrency Matrix**:
  - 1, 2, and 4 concurrent consumer workers/threads.
- **Hypotheses Under Evaluation**:
  - *Hypothesis 1 (Ingestion Throughput)*: Ingestion pipeline sustains at least 500 events/second during bulk replay on developer workstations; target 1,000 events/second under optimized batching.
  - *Hypothesis 2 (REST Query Latency)*: Pre-aggregated hourly zone stats query (`/api/v1/analytics/zone-hourly`) maintains p95 latency below 300 ms for up to 1,000 hourly buckets.
  - *Hypothesis 3 (Idempotent Scale & Zero Duplicates)*: Scaling from 1 to 4 consumers increases throughput without introducing race conditions, duplicate aggregate increments, or redundant alert generation.
- **Metrics Tracked**:
  - Ingestion events/second (sustained)
  - Producer acknowledgement latency (p50, p95, p99 in ms)
  - REST query latency (p50, p95, p99 in ms)
  - Peak RAM (RSS in MB) and CPU utilization (%)
  - Kafka consumer lag drain rate
  - Rejected event classification accuracy

### 2. Independent Correctness Baseline

To ensure data integrity cannot be falsified by service-level accounting bugs:
- Every benchmark run executes an independent PyArrow validation over the exact input slice.
- PyArrow compute kernels compute:
  - Exact accepted row count (rows satisfying validation rules)
  - Exact rejected row count (rows violating timestamp or coordinate bounds)
  - Exact total distance (thousandths of a mile)
  - Exact total fare (cents)
- The benchmark reconciles PostgreSQL database records against the PyArrow baseline. If any field differs by even 1 cent or 1 row, the benchmark fails immediately.

### 3. Architecture Artifacts and Release Deliverables

The portfolio release package includes:
1. **Logical Architecture Diagram**: Depicting language boundaries, Protobuf/gRPC contracts, Kafka topics, and partitioned database ownership.
2. **Deployment Architecture Diagram**: Depicting Docker Compose and Kubernetes Helm topologies with network isolation policies.
3. **Data-Flow & Failure-Sequence Diagram**: Detailing the journey of an event batch through gRPC, Kafka, Go analytics, Java alerts, and recovery from worker crash.
4. **Security Threat Model (STRIDE)**: Documenting container isolation, non-root execution, role separation (Analyst/Operator/Admin), and secrets management.
5. **Operational Runbook**: Runbook for cluster deployment, consumer lag alerts, replay triggers, and disaster recovery drills.
6. **Technical Interview Presentation**: Structured 10-slide outline for technical deep-dive interviews explaining design decisions, measured bottlenecks, trade-offs, and future work.

## Consequences

- All performance numbers published in documentation are backed by reproducible code and recorded hardware context.
- System throughput limits and database bottlenecks are transparently documented as engineering lessons.
- The repository stands as a comprehensive, production-grade distributed systems portfolio case study.
