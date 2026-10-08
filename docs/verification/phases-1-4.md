# Phases 1–4 verification record

Verified locally on 2026-10-08. This record reports executed evidence; proposed benchmark targets are not presented as measurements.

## Phase 1 — adapter and deterministic identity

- Official source: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`
- Local file: `data/yellow_tripdata_2024-01.parquet` (excluded by `data/.gitignore`)
- Size: 49,961,641 bytes
- SHA-256: `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510`
- PyArrow metadata: 2,964,624 rows, 3 row groups, 19 source columns
- Full adapter pass: 2,964,568 accepted; 56 rejected; all 56 rejection codes were `DROPOFF_BEFORE_PICKUP`
- Official lookup: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Lookup size/SHA-256: 12,331 bytes / `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`
- The lookup loader verifies the checksum, required columns, unique positive IDs, and the complete `1..265` ID set.
- Replay uses a copied, hash-bound snapshot. Source replacement after replay begins cannot change bytes associated with the source hash.
- Python 3.13.1, PyArrow 24.0.0: 22 tests passed.

Full inspection command:

```powershell
$env:PYTHONPATH="$PWD\tools\replay-python\src"
python -m streamforge_replay.cli data\yellow_tripdata_2024-01.parquet --batch-size 100000
```

## Phase 2 — Go and PostgreSQL

- Go 1.24 module with domain states, repository boundaries, seven forward migrations, and PostgreSQL access through pgx v5.
- The event transaction locks the run row against terminal transitions and claims `(run_id,event_id)` before outcome-specific side effects.
- Live PostgreSQL tests cover retry stability, cross-run duplicates, forced rollback, durable rejection, concurrent accepted/rejected requests, terminal-transition concurrency, and fresh-schema forward migrations.
- All Go tests and `go vet ./...` passed against PostgreSQL 17 in Docker Compose.

## Phase 3 — Protobuf and gRPC

- Versioned `streamforge.ingest.v1` schema and generated Go/Python bindings are present.
- Batch and request-size bounds, stable per-event outcomes, bounded exponential retry with jitter, and database-commit acknowledgments are implemented.
- Unexpected storage errors are logged with request ID and sanitized before returning to clients.
- Loopback-only listener enforcement prevents accidental unauthenticated network exposure.
- The live response-loss test discards the first successful response after commit; retry leaves one event, one outcome, and one aggregate contribution.

## Phase 4 — REST and dashboard

- Dataset registration/list/get, run create/list/get/complete/cancel, hourly analytics, rejection pagination, and health endpoints are implemented.
- The read-only React/TypeScript dashboard displays datasets, recent run states and counters, hourly analytics, rejection evidence, data completeness, and persistent historical-replay labeling.
- The production dashboard build and desktop/mobile browser checks passed without console errors.
- `scripts/demo.ps1` executed a known 100-row Parquet fixture through REST registration, Python replay, gRPC ingestion, PostgreSQL persistence, completion, and REST analytics.
- Reconciliation result: 100 input, 90 accepted, 10 rejected, 90 aggregate trips — PASS.

Run the complete local demo:

```powershell
.\scripts\demo.ps1
```
