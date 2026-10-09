# ADR-006: Kafka at-least-once delivery with database idempotency

Status: accepted for Phase 5

## Decision

In Kafka mode, Go ingestion publishes versioned raw-event envelopes to
`streamforge.raw-events.v1` and returns `KAFKA_PUBLISHED` only after an all-ISR
broker acknowledgment. The stable source-derived `event_id` is the Kafka key.
No ordering dependency exists between distinct trip events, so this key spreads
a large dataset across partitions while routing retries of one event consistently.

The `streamforge-analytics-v1` consumer group processes records at least once.
It commits a record's Kafka offset only after the existing PostgreSQL event
transaction commits. A crash before that offset commit causes redelivery; the
database uniqueness constraints and durable run outcome make that redelivery a
no-op rather than a second aggregate contribution.

Malformed, unsupported, or permanently invalid messages are recorded in
`kafka_consumer_failures` before their offsets advance. Transient database
failures leave offsets unresolved and stop the consumer so process supervision
can restart it from the last committed position.

## Consequences

- Broker publication and database processing are distinct, observable stages.
- The system does not claim a distributed exactly-once transaction across Kafka
  and PostgreSQL.
- Kafka may contain duplicate records after ambiguous producer failures; this is
  expected and safe because PostgreSQL remains the idempotency boundary.
- Consumer lag is calculated from broker committed/end offsets, and automated
  crash/restart evidence covers termination on both sides of the database
  commit boundary.
