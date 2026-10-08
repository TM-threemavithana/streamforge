# ADR-001: Source-row event identity

Status: accepted for Phase 1

## Decision

Identify each replay event as SHA-256 of UTF-8 `nyc-yellow:v1:<source_sha256>:<zero_based_logical_row_number>`, with the source digest encoded as lowercase hexadecimal.

## Consequences

Repeated reads and retries of identical file bytes produce identical event IDs. Different source bytes intentionally create a different identity namespace. This prevents duplicate counting within a registered dataset but does not claim to deduplicate the same physical taxi trip across distinct source files.

