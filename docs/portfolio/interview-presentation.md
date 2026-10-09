# StreamForge 2.0: Technical Interview Presentation

A structured presentation deck outline designed for Senior and Staff Distributed Systems & Data Platform Engineering interviews.

---

## Slide 1: System Vision & Problem Statement

### Title: StreamForge 2.0: High-Throughput Mobility Streaming with Deterministic Invariants

### Key Talking Points:
- **The Challenge:** Building a distributed data pipeline capable of ingesting tens of millions of historical mobility records (NYC TLC dataset) with sub-second anomaly detection while guaranteeing **zero financial drift** and **strict idempotency**.
- **The Pitfalls of Traditional Streaming:** Floating-point rounding errors in floating fares, non-idempotent consumer re-drives inflating financial totals, and unsegregated database boundaries risking cross-domain pollution.
- **The StreamForge Mandate:** 
  - Fixed-point integer precision (cents & milli-miles).
  - Deterministic SHA-256 event and alert identities.
  - Provable empirical performance (> 9,000 eps sustained throughput).

---

## Slide 2: Polyglot Architecture & Intentional Trade-Offs

### Key Talking Points:
- **No Silver Bullet Runtime:** Instead of forcing one language across conflicting domains, we matched runtimes to workloads:
  - **Python 3.11 + PyArrow:** Vectorized C++ SIMD scanning of large Parquet chunks.
  - **Go 1.24 (`franz-go` + `pgx`):** Low-latency, lightweight ingestion gatekeeper (<75 MB RSS) and high-throughput Kafka producer.
  - **Java 17 / Spring Boot 4:** Complex business rule engine with versioned thresholds and dynamic alert evaluations.
  - **PostgreSQL 16 (Segregated):** Isolated physical databases (`streamforge` vs `streamforge_alerts`) preventing shared-database coupling.
- **Microservice Communication:** gRPC Protobuf for bounded batch ingestion; Apache Kafka 3.8 (KRaft) as the durable event backbone.

---

## Slide 3: Deterministic Identity & Financial Invariant Contracts

### Key Talking Points:
- **Deterministic Event Identity Formulation:**
  $$\text{event\_id} = \text{SHA-256}\left(\text{"nyc-yellow:v1:"} + \text{source\_sha256} + \text{":"} + \text{row\_number}\right)$$
  - Independent of wall-clock time, system clocks, or host machine.
  - Guarantees replay idempotency: re-running a 1M event dataset produces identical keys.
- **Deterministic Alert Identity Formulation:**
  $$\text{alert\_id} = \text{SHA-256}\left(\text{event\_id} + \text{"\|"} + \text{rule\_id} + \text{"\|"} + \text{rule\_version}\right)$$
  - Ensures rule engine updates can be audited and prevents re-alerting on the same trip anomaly.
- **Financial Exactness:**
  - Standard floating-point IEEE-754 arithmetic introduces drift over millions of additions (`0.1 + 0.2 != 0.3`).
  - StreamForge stores all fares as `INT64 cents` and distances as `INT64 milli-miles`.

---

## Slide 4: Deep Dive: The 117x Kafka Producer Optimization

### Key Talking Points:
- **The Finding:** Initial profiling showed the pipeline capped at only **83.6 events/sec**.
- **Root Cause Analysis:**
  - `franz-go` was invoked serially inside a record loop: `p.client.ProduceSync(ctx, record)`.
  - In a 500-event batch, this triggered 500 individual synchronous network round-trips to Kafka.
  - At 2.4 ms RTT, each batch took ~1,200 ms to commit.
- **The Architectural Fix:**
  - Refactored Go core interfaces to support batch publishing: `p.client.ProduceSync(ctx, records...)`.
  - Allowed `franz-go` to pipeline all 500 records into a single Kafka Produce RPC request.
- **Empirical Breakthrough:**
  - Batch commit latency plummeted from **1,200 ms to 28.37 ms**.
  - Sustained throughput exploded from **83.6 eps to 9,792.9 eps** (**117.1x speedup**).

---

## Slide 5: Empirical Benchmark Matrix & PyArrow Reconciliation

### Key Talking Points:
- **Empirical Evidence vs Assertions:** Every claim backed by reproducible benchmark harnesses (`tools/benchmarks/benchmark_harness.py`).
- **Benchmark Summary (10,000 Event Official Slice):**
  - **Sustained Throughput:** 9,792.9 events/sec (Target: >= 1,000 eps).
  - **Producer Latency (p50 / p95 / p99):** 28.4 ms / 36.0 ms / 36.0 ms.
  - **REST Query Latency (p50 / p95):** 7.3 ms / 30.0 ms (Target: < 300 ms).
  - **Worker Memory Footprint:** 73.9 MB RSS (Target: < 512 MB).
- **Independent PyArrow Baseline:**
  - Ground truth computed in-memory via PyArrow directly from Parquet.
  - Zero discrepancy ($\Delta = 0$) across total rows, accepted trips, and financial sums.

---

## Slide 6: Resiliency, Idempotency & Disaster Recovery Drills

### Key Talking Points:
- **Idempotent Replay Guarantee:**
  - Replaying an identical dataset results in `run_event_outcomes` registering `DUPLICATE`.
  - `hourly_zone_stats` aggregations are bypassed on duplicates, preventing financial overcounting.
- **Automated Disaster Recovery Drill (`scripts/backup_restore_drill.py`):**
  - Automated CI-executable DR script dumps production databases and restores to an ephemeral instance.
  - Runs 100% hash and invariant checks across all tables to guarantee non-corrupted backups.
  - Validates RPO (< 5 min) and RTO (< 15 min).

---

## Slide 7: Security Posture & Defense in Depth

### Key Talking Points:
- **STRIDE Threat Modeling:**
  - Ingestion limits: 4 MiB max gRPC frame size; 500 max batch size.
  - Database isolation: `streamforge_user` cannot access `streamforge_alerts`; separate credentials.
- **Kubernetes Pod Security Standards (Restricted Profile):**
  - Non-root user execution (`UID 10001:10001`).
  - Read-only root filesystem (`readOnlyRootFilesystem: true`).
  - All Linux capabilities dropped (`drop: [ALL]`).
  - Validated via automated Helm manifest scanner (`scripts/validate_helm_manifests.py`).

---

## Slide 8: Technical Retrospective & Scalability Roadmap

### Key Talking Points:
- **What Worked Exceptionally Well:**
  - Separating ingestion gatekeeping from analytical aggregation.
  - Pipelining Kafka batch produces via `franz-go`.
  - Deterministic hash derivation for automated replay deduplication.
- **Current Limits & Next Steps:**
  - *Current Limit:* 6 Kafka partitions cap analytics worker scale at 6 pods.
  - *Next Step:* Scale topic to 32 partitions with key-hash distribution.
  - *Next Step:* Migrate PostgreSQL pre-aggregations to TimescaleDB hypertables for multi-terabyte data horizons.

---

## Anticipated Technical Interview Questions & Answers

### Q1: Why did you choose separate PostgreSQL databases instead of schemas in one database?
> **Answer:** "Physical database separation provides a hard operational boundary. It allows distinct connection pooling limits, independent backup/restore cycles, distinct replication topologies, and enforces least-privilege database user permissions (`streamforge_user` vs `streamforge_alerts_user`) where neither service can ever query or lock the other's tables, preventing cross-domain cascade failures."

### Q2: How does StreamForge handle Kafka consumer rebalance storms?
> **Answer:** "We use static consumer group membership via `group.instance.id` where applicable, along with cooperative sticky partition assignment. Furthermore, because our database writes use transactional idempotency (`ON CONFLICT DO NOTHING`), duplicate partition deliveries caused by transient rebalances are safe and generate zero side effects."

### Q3: What happens if an event arrives out of order?
> **Answer:** "Our pre-aggregations in `hourly_zone_stats` are keyed by `(pickup_location_id, hour_bucket)`. Because SQL `UPSERT` operations additively increment counts and sums (`SET trip_count = hourly_zone_stats.trip_count + EXCLUDED.trip_count`), the operations are commutative and associative. Arrival order does not affect the final mathematical sum."
