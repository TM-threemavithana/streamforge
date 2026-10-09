# StreamForge 2.0: Logical Architecture

## 1. System Overview

StreamForge is a distributed, polyglot data pipeline built to process and analyze large-scale historical mobility datasets with deterministic identity, strict idempotency, and sub-second anomaly alerting.

```mermaid
flowchart TD
    subgraph Ingestion["Ingestion Tier (Python / PyArrow)"]
        Parquet["NYC TLC Yellow Taxi Parquet"] --> Normalizer["Bounded Adapter\nUTC Normalization"]
        Normalizer --> ReplayCLI["gRPC Client\nStream Replay"]
    end

    subgraph CoreService["Core Ingestion & Catalog Tier (Go 1.24)"]
        gRPCServer["gRPC Server :50051\n(Max 4 MiB / 500 events)"]
        RESTServer["REST API Server :8080\nDatasets / Runs / Analytics"]
        MetricsEndpoint["Prometheus Metrics\nGET /metrics"]
    end

    subgraph MessagingTier["Event Broker Tier (Apache Kafka 3.8 / KRaft)"]
        RawEventsTopic["streamforge.raw-events.v1\n(6 Partitions / Key: event_id)"]
    end

    subgraph AnalyticsWorkerTier["Analytics Stream Processing Tier (Go 1.24)"]
        AnalyticsConsumer["Analytics Consumer\nGroup: streamforge-analytics-v1"]
    end

    subgraph AlertsWorkerTier["Anomaly Rules Engine Tier (Java 17 / Spring Boot 4)"]
        AlertsConsumer["Alerts Consumer\nGroup: streamforge-alerts-v1"]
        RulesEngine["Stateful Rules Engine\nHIGH_FARE, LONG_DISTANCE, UNUSUAL_DURATION"]
        AlertsAPI["Alerts REST API :8081\nRules & Alerts Management"]
    end

    subgraph StorageTier["Data Persistence Tier (PostgreSQL 16)"]
        subgraph CoreDB["streamforge (Owner: streamforge_user)"]
            DatasetsTable["datasets"]
            RunsTable["replay_runs"]
            TripsTable["trip_events"]
            OutcomesTable["run_event_outcomes"]
            HourlyStatsTable["hourly_zone_stats"]
        end

        subgraph AlertsDB["streamforge_alerts (Owner: streamforge_alerts_user)"]
            AlertRulesTable["alert_rules"]
            AlertsTable["alerts"]
            AlertOutcomesTable["alert_event_outcomes"]
            ConsumerFailuresTable["alert_consumer_failures"]
        end
    end

    subgraph PresentationTier["User Interface Tier (React 19 / TypeScript / Vite)"]
        Dashboard["StreamForge Operational Dashboard :8080\n(Reverse Proxy to Go :8080 & Java :8081)"]
    end

    ReplayCLI -->|Protobuf over gRPC| gRPCServer
    gRPCServer -->|Batch Produce| RawEventsTopic
    gRPCServer -->|Metadata Check| RunsTable

    RawEventsTopic -->|At-Least-Once Pull| AnalyticsConsumer
    AnalyticsConsumer -->|Atomic Aggregation| HourlyStatsTable
    AnalyticsConsumer -->|Durable Outcome| OutcomesTable

    RawEventsTopic -->|At-Least-Once Pull| AlertsConsumer
    AlertsConsumer --> RulesEngine
    RulesEngine -->|Deterministic Alert ID| AlertsTable
    AlertsConsumer -->|Offset Commitment| AlertOutcomesTable

    RESTServer --> CoreDB
    AlertsAPI --> AlertsDB

    Dashboard -->|/api/*| RESTServer
    Dashboard -->|/api/v1/alerts-service/*| AlertsAPI
```

## 2. Component Boundaries & Responsibilities

| Component | Language / Runtime | Protocol | Primary Responsibility | Ownership Boundary |
| --- | --- | --- | --- | --- |
| **Replay CLI** | Python 3.11 / PyArrow | CLI / gRPC | Reads NYC TLC Parquet in bounded batches, computes SHA-256 identities, strips wall-clock ambiguities. | Source file reading and client-side retry logic. |
| **Core Service** | Go 1.24 / pgx | gRPC (:50051), HTTP (:8080) | Validates Protobuf envelopes, publishes batches to Kafka, exposes dataset/run management and aggregate query APIs. | Ingestion gatekeeper, `streamforge` database. |
| **Analytics Consumer** | Go 1.24 / franz-go | Kafka Consumer | Consumes `streamforge.raw-events.v1`, performs idempotent deduplication, updates pre-aggregated `hourly_zone_stats`. | Event aggregation, `run_event_outcomes`. |
| **Alerts Service** | Java 17 / Spring Boot 4 | Kafka Consumer, HTTP (:8081) | Consumes `streamforge.raw-events.v1`, evaluates versioned anomaly rules, persists alerts with deterministic IDs. | `streamforge_alerts` database. |
| **Dashboard** | React 19 / TypeScript | HTTP / Nginx (:8080) | Visualizes zone-hourly metrics, Kafka lag, run progress, source rejections, and live anomaly alerts. | User presentation and routing proxy. |

## 3. Protocol & Identity Contracts

1. **Deterministic Event Identity**:
   ```text
   event_id = SHA-256("nyc-yellow:v1:" + source_sha256 + ":" + row_number)
   ```
2. **Deterministic Alert Identity**:
   ```text
   alert_id = SHA-256(event_id + "|" + rule_id + "|" + rule_version)
   ```
3. **Kafka Envelope**:
   Frozen schema `streamforge.raw-event:v1` containing schema version, kind (`TRIP` or `SOURCE_REJECTION`), trip payload, and rejection context.
