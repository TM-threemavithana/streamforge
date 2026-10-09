# StreamForge 2.0: Operations & Disaster Recovery Runbooks

## 1. Incident Severity Matrix

| Severity | Definition | Target MTTA | Target MTTR | Escalation Path |
| --- | --- | --- | --- | --- |
| **SEV-1** | Total Ingestion Blackout or Primary Database Corruption. Zero events processing. | < 5 mins | < 30 mins | On-Call Lead + Platform Architect |
| **SEV-2** | Critical Consumer Lag (> 50,000 records) or Anomaly Detection Failure. Core ingestion active. | < 15 mins | < 60 mins | Data Infrastructure Team |
| **SEV-3** | Single Worker Pod Crashloop or Non-blocking Dashboard UI degradation. | < 30 mins | < 4 hours | Service Owner |
| **SEV-4** | Minor telemetry drift or non-impacting log warnings. | Next Business Day | Next Sprint | Team Backlog |

---

## 2. Runbook 1: Kafka Consumer Lag Triage & Auto-Scaling

### Symptoms & Alerts:
- Prometheus Alert: `StreamForgeHighConsumerLag` firing (`total_lag > 5000` for > 3 minutes).
- REST Endpoint: `GET /api/v1/operations/kafka-lag` reports accumulating partition lag.

### Diagnostic Steps:
1. **Query Kafka Consumer Lag via REST API:**
   ```bash
   curl -s http://localhost:8080/api/v1/operations/kafka-lag | jq .
   ```
2. **Inspect Consumer Group Partition Allocation:**
   ```bash
   docker exec -it docker-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --describe --group streamforge-analytics-v1
   ```
3. **Check PostgreSQL Transaction Contention:**
   Inspect if database lock contention is stalling batch upserts:
   ```sql
   SELECT pid, now() - query_start AS duration, query, state 
   FROM pg_stat_activity 
   WHERE state != 'idle' AND query LIKE '%hourly_zone_stats%';
   ```

### Remediation:
- **Scale Analytics Worker Replicas (up to partition count of 6):**
  ```bash
  kubectl scale deployment streamforge-analytics --replicas=6 -n streamforge
  ```
- **Tune Postgres Connection Pool (if connection starved):**
  Increase `DB_MAX_CONNS` from 25 to 50 in `streamforge-core-config`.
- **Drain Verification:**
  Monitor lag until `total_lag == 0`:
  ```bash
  watch -n 2 'curl -s http://localhost:8080/api/v1/operations/kafka-lag | jq .total_lag'
  ```

---

## 3. Runbook 2: Kafka Broker Outage & Partition Rebalance Recovery

### Symptoms:
- Go Core logs: `ProduceSync failed: kafka connection reset by peer`.
- Consumer logs: `Rebalance in progress; revoking partitions`.

### Diagnostic Steps:
1. **Verify Broker Health:**
   ```bash
   docker exec -it docker-kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
     --bootstrap-server localhost:9092
   ```
2. **Check Topic Partition ISR (In-Sync Replicas):**
   ```bash
   docker exec -it docker-kafka-1 /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server localhost:9092 \
     --describe --topic streamforge.raw-events.v1
   ```

### Remediation:
- If a broker pod is unresponsive, perform a rolling restart:
  ```bash
  kubectl rollout restart statefulset/streamforge-kafka -n streamforge
  ```
- Ensure consumer pods have completed rebalance by verifying steady state in consumer logs:
  `assigned partitions: [0, 1, 2, 3, 4, 5]`.

---

## 4. Runbook 3: Poison Pill & Deserialization Isolation (Dead-Letter Triage)

### Symptoms:
- Spring Boot Alerts logs: `Failed to deserialize record payload at offset X`.
- Metrics: `alert_consumer_failures_total` incrementing.

### Diagnostic Steps:
1. **Query Dead-Letter Table in Alerts Database:**
   ```sql
   SELECT failure_id, topic, partition, kafka_offset, error_reason, failed_at, 
          encode(payload_hex, 'hex') AS payload_sample
   FROM alert_consumer_failures 
   ORDER BY failed_at DESC 
   LIMIT 10;
   ```
2. **Analyze Malformed Envelope:**
   Identify if payload is an unauthorized schema version or invalid Protobuf envelope.

### Remediation:
- **Quarantine Confirmation:** Verify that the consumer group skipped the offending offset without crashing.
- **Redrive / Patch:** If the schema failure was due to an unannounced client version change, update the Java Protobuf deserializer and replay quarantined payloads via the dead-letter redrive tool.

---

## 5. Runbook 4: Database Disaster Recovery & Automated Point-in-Time Restore

StreamForge mandates automated disaster recovery drills to guarantee data durability and schema invariant integrity.

### RPO & RTO Targets:
- **Recovery Point Objective (RPO):** < 5 minutes (via continuous WAL archiving).
- **Recovery Time Objective (RTO):** < 15 minutes (automated script execution).

### Step-by-Step Restoration Drill Procedure:

1. **Trigger Automated Database Backup:**
   Execute the PowerShell backup script targeting both `streamforge` and `streamforge_alerts`:
   ```powershell
   powershell.exe -ExecutionPolicy Bypass -File scripts/backup-db.ps1
   ```
   *Expected output: Generates timestamped `.sql.gz` dumps in `deploy/backups/`.*

2. **Execute Disaster Recovery Drill with 100% Invariant Validation:**
   Run the automated Python DR drill script:
   ```bash
   python scripts/backup_restore_drill.py
   ```
   *Execution workflow performed by drill:*
   - Performs fresh snapshot of primary databases.
   - Spins up ephemeral recovery verification database (`streamforge_dr_test`).
   - Restores snapshot dump completely.
   - Executes exact row count and hash invariant checks across `datasets`, `replay_runs`, `trip_events`, `run_event_outcomes`, `hourly_zone_stats`, `alert_rules`, and `alerts`.
   - Verifies 100% data parity and drops ephemeral recovery container.

3. **Emergency Production Database Restore (If Primary DB Corrupted):**
   ```bash
   # 1. Stop all ingestion and consumer traffic
   kubectl scale deployment streamforge-core streamforge-analytics streamforge-alerts --replicas=0 -n streamforge

   # 2. Restore primary database from verified backup
   gunzip -c deploy/backups/streamforge_backup_latest.sql.gz | psql -h $DB_HOST -U postgres -d streamforge

   # 3. Restore alerts database
   gunzip -c deploy/backups/streamforge_alerts_backup_latest.sql.gz | psql -h $DB_HOST -U postgres -d streamforge_alerts

   # 4. Resume microservices
   kubectl scale deployment streamforge-core streamforge-analytics streamforge-alerts --replicas=2 -n streamforge
   ```

---

## 6. Incident Post-Mortem Template

```markdown
# StreamForge Incident Post-Mortem

**Date:** YYYY-MM-DD  
**Severity:** SEV-X  
**Incident Commander:** [Name]  
**Duration / Outage Window:** HH:MM to HH:MM UTC  

### Executive Summary
[Brief description of incident impact, affected services, and resolution]

### Root Cause Analysis (5 Whys)
1. Why did the service fail?
2. Why was that condition triggered?
3. Why did existing alerts not prevent it?
4. Why was backpressure inadequate?
5. Why did our test suite not detect this scenario?

### Preventative Action Items
- [ ] JIRA-XXX: Implement rate-limiting backpressure on gRPC ingestion.
- [ ] JIRA-YYY: Add automated consumer lag alerts in Helm PrometheusRule.
- [ ] JIRA-ZZZ: Update DR drill runbook with newly discovered edge cases.
```
