# StreamForge 2.0: Data Flow & Failure Sequence Models

## 1. End-to-End Happy Path Data Flow

The following sequence diagram details the end-to-end lifecycle of an event batch from NYC TLC Parquet file reading to durable analytics aggregation and anomaly alert generation.

```mermaid
sequenceDiagram
    autonumber
    actor CLI as Replay CLI (Python / PyArrow)
    participant Core as Core Ingestion (Go gRPC :50051)
    participant DB_Core as PostgreSQL (streamforge)
    participant Broker as Apache Kafka (raw-events.v1)
    participant Analytics as Analytics Consumer (Go)
    participant Alerts as Alerts Consumer (Java / Spring)
    participant DB_Alerts as PostgreSQL (streamforge_alerts)

    Note over CLI: Vectorized Parquet Scan & Normalization
    CLI->>Core: RegisterDataset(name, sha256, schema)
    Core->>DB_Core: INSERT INTO datasets ... ON CONFLICT DO NOTHING
    Core-->>CLI: DatasetRegistered(id)

    CLI->>Core: StartRun(dataset_id, expected_events)
    Core->>DB_Core: INSERT INTO replay_runs (status='RUNNING')
    Core-->>CLI: RunStarted(run_id)

    loop Batches of 500 Events
        Note over CLI: Compute deterministic SHA-256 event_id
        CLI->>Core: StreamEvents(run_id, events[1..500])
        Note over Core: Pipelined Batch Produce (franz-go)
        Core->>Broker: ProduceSync(topic, batch[1..500], key=event_id)
        Broker-->>Core: ProducerAck(partition, offsets)
        Core-->>CLI: StreamEventsResponse(accepted=500, rejected=0)
    end

    par Parallel Stream Processing
        Broker->>Analytics: FetchBatch(partition_offsets)
        Note over Analytics: Deduplicate against run_event_outcomes
        Analytics->>DB_Core: BEGIN TX
        Analytics->>DB_Core: INSERT INTO trip_events ... ON CONFLICT DO NOTHING
        Analytics->>DB_Core: INSERT INTO run_event_outcomes ... ON CONFLICT DO NOTHING
        Analytics->>DB_Core: INSERT INTO hourly_zone_stats ... ON CONFLICT (zone, hour) DO UPDATE
        Analytics->>DB_Core: COMMIT TX
        Analytics->>Broker: CommitOffsets()
    and Anomaly Alert Processing
        Broker->>Alerts: FetchBatch(partition_offsets)
        loop Each Event in Batch
            Note over Alerts: Evaluate In-Memory Anomaly Rules
            opt Trip Exceeds Rule Threshold
                Note over Alerts: Compute deterministic alert_id = SHA256(event_id|rule_id|v)
                Alerts->>DB_Alerts: INSERT INTO alerts ... ON CONFLICT (alert_id) DO NOTHING
            end
            Alerts->>DB_Alerts: INSERT INTO alert_event_outcomes ... ON CONFLICT DO NOTHING
        end
        Alerts->>Broker: CommitOffsets()
    end

    CLI->>Core: CompleteRun(run_id)
    Core->>DB_Core: Verify COUNT(run_event_outcomes) == expected_events
    Core->>DB_Core: UPDATE replay_runs SET status='COMPLETED'
    Core-->>CLI: RunCompleted(status='COMPLETED')
```

---

## 2. Failure Sequence 1: Transient gRPC Producer Network Failure & Backoff

When network latency spikes or a broker election causes transient gRPC timeouts, the Replay client executes bounded exponential backoff with jitter.

```mermaid
sequenceDiagram
    autonumber
    actor CLI as Replay CLI
    participant Core as Core Ingestion (Go)
    participant Broker as Kafka Broker

    CLI->>Core: StreamEvents(batch #42, events 20501..21000)
    Core->>Broker: ProduceSync(records)
    Note over Broker: Broker Leader Election in progress...
    Broker--xCore: LeaderNotAvailable / Timeout
    Core--xCLI: gRPC UNAVAILABLE: kafka produce timeout (5000ms)

    Note over CLI: Backoff Attempt 1 (delay 500ms + jitter)
    CLI->>Core: StreamEvents(batch #42, events 20501..21000)
    Core->>Broker: ProduceSync(records)
    Broker-->>Core: ProducerAck(partition 2, offsets 8100..8599)
    Core-->>CLI: StreamEventsResponse(accepted=500)
    Note over CLI: Batch successfully committed; resumes normal replay rate
```

---

## 3. Failure Sequence 2: Replay Run Collision & Strict Idempotency

If an operator re-runs a historical dataset or replays overlapping slices, the pipeline guarantees **zero double-counting** across all financial metrics and trip counts.

```mermaid
sequenceDiagram
    autonumber
    actor CLI as Replay CLI
    participant Core as Core Ingestion
    participant Broker as Kafka Broker
    participant Analytics as Analytics Consumer
    participant DB as PostgreSQL (streamforge)

    Note over CLI: Operator restarts replay for already processed batch
    CLI->>Core: StreamEvents(events with duplicate event_id)
    Core->>Broker: ProduceSync(batch)
    Broker-->>Core: ProducerAck
    Core-->>CLI: StreamEventsResponse(accepted=500)

    Broker->>Analytics: FetchRecord(event_id="nyc-yellow:v1:...")
    Analytics->>DB: BEGIN TRANSACTION

    Analytics->>DB: INSERT INTO trip_events (event_id, ...) VALUES (...) ON CONFLICT (event_id) DO NOTHING
    Note over DB: Conflict detected! Zero rows inserted into trip_events.

    Analytics->>DB: INSERT INTO run_event_outcomes (run_id, event_id, outcome) VALUES (run_id, event_id, 'DUPLICATE') ON CONFLICT (run_id, event_id) DO NOTHING
    Note over DB: Outcome registered as DUPLICATE

    Note over Analytics: Skip hourly_zone_stats aggregation increment
    Analytics->>DB: COMMIT TRANSACTION
    Analytics->>Broker: CommitOffsets()

    Note over DB: Result: hourly_zone_stats counters remain mathematically pristine
```

---

## 4. Failure Sequence 3: Consumer Crash & Partition Rebalance

When an analytics worker pod crashes mid-batch, Kafka consumer group rebalances ensure at-least-once recovery without duplicate financial side effects.

```mermaid
sequenceDiagram
    autonumber
    participant Broker as Kafka Broker
    participant Worker1 as Analytics Pod 1 (Crash Candidate)
    participant Worker2 as Analytics Pod 2 (Surviving Pod)
    participant DB as PostgreSQL (streamforge)

    Broker->>Worker1: FetchBatch(offsets 1000..1099)
    Worker1->>DB: BEGIN TX
    Worker1->>DB: INSERT INTO trip_events (offsets 1000..1050)
    Worker1->>DB: COMMIT TX
    Note over Worker1: OOMKilled / Pod Crash before offset commit!
    Worker1--xBroker: Connection Terminated

    Note over Broker: Broker detects Heartbeat Timeout (45s)
    Note over Broker: Trigger Consumer Group Rebalance
    Broker->>Worker2: Assign Partitions [0, 1] to Worker2
    Note over Broker: Last committed offset was 999
    Broker->>Worker2: FetchBatch(offsets 1000..1099)

    Worker2->>DB: BEGIN TX
    Note over Worker2: Re-process offsets 1000..1050
    Worker2->>DB: INSERT INTO trip_events ... ON CONFLICT DO NOTHING
    Note over DB: Rows 1000..1050 already exist -> No-op
    Worker2->>DB: INSERT INTO trip_events (offsets 1051..1099)
    Note over DB: Rows 1051..1099 inserted cleanly
    Worker2->>DB: COMMIT TX

    Worker2->>Broker: CommitOffset(1099)
    Note over Broker: Partition offset successfully advanced to 1100
```

---

## 5. Failure Sequence 4: Poison Pill / Deserialization Isolation (Dead-Letter Handling)

When an unparseable or malicious byte payload arrives on the Kafka topic, workers isolate the record into durable audit tables rather than crashlooping the consumer group.

```mermaid
sequenceDiagram
    autonumber
    participant Broker as Kafka Topic (raw-events.v1)
    participant JavaAlerts as Alerts Service Consumer
    participant AlertsDB as PostgreSQL (streamforge_alerts)

    Broker->>JavaAlerts: FetchRecord(corrupted_byte_payload)
    Note over JavaAlerts: JSON / Protobuf Deserialization Exception
    JavaAlerts->>AlertsDB: INSERT INTO alert_consumer_failures (topic, partition, offset, payload_hex, error_reason) VALUES (...)
    Note over AlertsDB: Failure permanently quarantined for operator inspection
    JavaAlerts->>Broker: CommitOffset(poison_pill_offset)
    Note over JavaAlerts: Consumer advances cleanly to next valid record
```
