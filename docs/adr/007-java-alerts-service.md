# ADR-007: Java Alerts Service Boundary and Contract

Status: accepted for Phase 6

## Decision

To support the Phase 6 business capability of rule-based alerting, we will introduce `services/alerts-java`, an independent Spring Boot service that consumes the existing Kafka raw-event topic.

We freeze the following contracts and boundaries before implementation:

### 1. Supported `streamforge.raw-event:v1` envelope fields
The Java consumer will deserialize the existing envelope without altering Go publisher semantics. Supported JSON fields are:
- `schema_version`: Must be exactly `"streamforge.raw-event:v1"`.
- `kind`: `"TRIP"` or `"SOURCE_REJECTION"`.
- `trip`: For `"TRIP"`, contains `DatasetID`, `RunID`, `EventID`, `SourceSHA256`, `SourceRowNumber`, `PickupAt`, `DropoffAt`, `PickupZoneID`, `DropoffZoneID`, `DistanceMilliMiles`, and `FareCents`.
- `rejection`: For `"SOURCE_REJECTION"`, contains rejection context (ignored by rules engine initially).

### 2. Initial Rule Kinds and Threshold Configuration
Thresholds will be explicitly configured in the database, not hardcoded. Initial rule kinds to be supported:
- `HIGH_FARE`: e.g., default threshold > 10000 cents ($100.00).
- `LONG_DISTANCE`: e.g., default threshold > 50000 milli-miles (50 miles).
- `UNUSUAL_DURATION`: e.g., duration > 10800 seconds (3 hours) or negative duration.

Rule definitions will be stored in `alert_rules` and include `rule_id`, `rule_version`, `kind`, `parameters` (JSON), and `enabled` flag.

### 3. Deterministic Alert-ID Canonical Encoding
Alert identity must be deterministic to ensure replay idempotency.
Encoding: `SHA-256` of `event_id + "|" + rule_id + "|" + rule_version` formatted as lowercase hex.
*Test vector:*
- event_id: `evt123`
- rule_id: `high_fare`
- rule_version: `1`
- Input string: `evt123|high_fare|1`
- Expected SHA-256 Hex: `8dfb7762b322a3afeb7b79dae8fc73b4d2ea523dc033f7ccb2f6efba9830da0f`

### 4. Alert Database Ownership and Transaction Boundary
The Java service will own a separate PostgreSQL database/schema (`streamforge_alerts`).
It will write to:
- `alert_rules`
- `alerts`
- `alert_event_outcomes`
- `alert_consumer_failures`

**Transaction boundary**: Kafka offsets advance *only after* the transaction containing the alert (or no-op) and `alert_event_outcomes` successfully commits to the database.

### 5. Consumer-Group Name, Retry, and Permanent-Failure Policy
- **Group Name**: `streamforge-alerts-v1`
- **Retry Classification**: Transient errors (e.g., DB connection loss, broker timeout) will block offset advancement and trigger process restart. Permanent errors (e.g., malformed JSON, missing required fields) will be caught.
- **Permanent-Failure Policy**: Write permanent failures to `alert_consumer_failures` within the DB transaction, then advance the Kafka offset.

### 6. Minimum Read/Write API Surface
The service will expose the following REST endpoints (prefixed with `/api/v1/alerts-service`):
- `GET /rules`: List all rules and versions.
- `POST /rules`: Create a new rule or new version of an existing rule.
- `PATCH /rules/{id}/status`: Enable/disable a rule.
- `GET /alerts`: List generated alerts (with pagination, filtering by `dataset_id` or `rule_id`).

## Consequences
- The Go core service and Java alerts service remain perfectly decoupled, communicating only via Kafka.
- Reprocessing historical runs safely replays events through the alerts consumer without creating duplicate alerts.
- Cross-language payload compatibility is locked to `streamforge.raw-event:v1`.
