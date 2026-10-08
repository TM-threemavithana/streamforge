# StreamForge 2.0

StreamForge is a mobility operations and data-quality portfolio project. It replays the official NYC TLC Yellow Taxi January 2024 **historical** Parquet dataset; it is not live vehicle tracking.

The Python/PyArrow adapter reads bounded record batches, normalizes the six authoritative source fields, derives reproducible source-row event IDs, and reports explicit validation failures. The Go/PostgreSQL ingestion path uses database uniqueness and atomic aggregate updates so retries and crashes do not double-count accepted source events.

## Phase 1 contract

- Source identity: SHA-256 of the exact Parquet bytes.
- Event identity: SHA-256 of UTF-8 `nyc-yellow:v1:<lowercase-source-sha256>:<zero-based-logical-row>`.
- Source timestamps: naive values are New York wall time and become UTC. Ambiguous or nonexistent DST wall times are rejected unless future source metadata disambiguates them.
- Distance: integer thousandths of a mile, rounded half-up from decimal text.
- Fare: optional signed integer cents, rounded half-up. Negative fares are preserved for later anomaly policy.
- Zone validation: the CLI loads the official TLC taxi-zone lookup and refuses it if its pinned SHA-256, required columns, or IDs do not match.
- Memory: Parquet is projected to six columns and iterated in configurable batches; the inspection summary retains counters only.

## Run locally

Python 3.11+ is required.

```powershell
python -m pip install -e ".[dev]"
python -m pytest
streamforge-inspect .\data\yellow_tripdata_2024-01.parquet --batch-size 500
```

Do not commit the TLC Parquet file. Record its locally measured checksum and tool versions before treating a real-data run as verified.

## Roadmap

1. Phase 1 complete: official-file verification, pinned taxi-zone lookup, bounded adapter, deterministic identity, normalization, and rejection evidence.
2. Phase 2 complete: modular Go domain/application layer, pgx/PostgreSQL migrations, transaction idempotency, rollback, and concurrency tests.
3. Phase 3 complete: versioned Protobuf/gRPC ingestion, bounded retries, response-loss recovery, sanitized failures, and loopback-only exposure.
4. Phase 4 complete: REST datasets/runs/analytics/quality APIs, read-only React/TypeScript dashboard, and reconciled 100-row demo.
5. Introduce Kafka, the independent Java rules service, Compose, Kubernetes/Helm, observability, security, and performance evidence only in their planned stages.

## Phase 2 status

The Go domain model, replay-run transition rules, persistence interface, seven ordered PostgreSQL migrations, and pgx-backed transactional PostgreSQL repositories are present. The live PostgreSQL integration tests verify same-run retry stability, cross-run duplicate handling, exactly-once aggregate updates, forced rollback with no partial event, outcome, or aggregate change, durable source rejections, accepted/rejected request races, terminal-run transition races, and fresh forward migrations.

```powershell
cd deploy\docker
docker compose up -d --wait postgres

cd ..\..\services\core-go
$env:STREAMFORGE_TEST_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
go test ./... -count=1
```

Compose exposes the development database on loopback port `5433` by default to avoid colliding with a local PostgreSQL installation. Set `STREAMFORGE_POSTGRES_PORT` before starting Compose to choose another host port. If `docker compose up` reports that the Docker API pipe is missing on Windows, start Docker Desktop first. A valid Compose configuration is not evidence that the SQL migrations have executed; verify the live schema or run the integration tests against a newly initialized volume.

## Phase 3 status

The authoritative `streamforge.ingest.v1` Protobuf contract, generated Go/Python bindings, Go gRPC server, and bounded-retry Python client are implemented. `IngestBatch` and `ReportSourceRejections` accept 1–500 records per request with a 4 MiB server limit. Successful results distinguish `DATABASE_COMMITTED` from future broker acknowledgments.

Start the core service after PostgreSQL is healthy:

```powershell
cd services\core-go
$env:STREAMFORGE_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
go run ./cmd/core
```

Replay a registered historical dataset from another shell. Create the dataset and `RUNNING` run through the Phase 4 REST API first.

```powershell
streamforge-replay .\data\yellow_tripdata_2024-01.parquet `
  --dataset-id <dataset-uuid> `
  --run-id <run-uuid> `
  --grpc-target 127.0.0.1:50051 `
  --batch-size 500
```

The live cross-language integration test starts the Go gRPC server, invokes it with the generated Python client, discards the first success response after the database commit, and verifies that the automatic retry leaves exactly one event, outcome, and aggregate contribution. Protobuf generator versions and regeneration guidance are recorded in `tools/proto/README.md`.

## Phase 4 API status

The core process also serves the versioned REST API on `127.0.0.1:8080` by default. Set `STREAMFORGE_HTTP_ADDR` to override it. Implemented endpoints are:

- `POST /api/v1/datasets`, `GET /api/v1/datasets`, and `GET /api/v1/datasets/{id}`
- `POST /api/v1/runs`, `GET /api/v1/runs`, `GET /api/v1/runs/{id}`, `POST /complete`, and `POST /cancel`
- `GET /api/v1/analytics/zone-hourly`
- `GET /api/v1/quality/rejections`
- `GET /health/live` and `GET /health/ready`

Dataset registration is idempotent by source type and SHA-256. Creating a run returns it in `RUNNING` state so the replay client can begin immediately. Completing a run requires `expected_input_count` to equal the number of durable per-event outcomes; otherwise the API returns `409 CONFLICT`. Analytics use an inclusive `start`, exclusive `end`, optional pickup-zone filter, and bounded result limits. Dataset and rejection listings use opaque cursors.

```powershell
Invoke-RestMethod -Method Post -ContentType 'application/json' `
  -Uri http://127.0.0.1:8080/api/v1/datasets `
  -Body (@{
    source_type = 'nyc-yellow'
    source_sha256 = '<measured-lowercase-sha256>'
    source_schema_version = 'v1'
    filename = 'yellow_tripdata_2024-01.parquet'
    source_size_bytes = (Get-Item '.\data\yellow_tripdata_2024-01.parquet').Length
  } | ConvertTo-Json)
```

The read-only React/TypeScript dashboard is available in `apps/dashboard`. It labels the source as a historical replay, loads datasets, analytics, and quality rejections from the REST API, and keeps UTC and data-completeness state visible.

```powershell
cd apps\dashboard
npm install
npm run dev
```

The Vite development server listens on `127.0.0.1:4173` and proxies `/api` and `/health` to the local core service on `127.0.0.1:8080`. Set `VITE_STREAMFORGE_API_BASE` for a different same-origin or CORS-enabled API base.

## Verified official source and complete demo

The locally measured January 2024 source SHA-256 is `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510`. A full pass processed 2,964,624 rows: 2,964,568 accepted and 56 rejected as `DROPOFF_BEFORE_PICKUP`. The official taxi-zone lookup is pinned to SHA-256 `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`.

Run the reproducible 100-row end-to-end demonstration:

```powershell
.\scripts\demo.ps1
```

The executed verification record, exact versions, commands, and reconciliation results are in [docs/verification/phases-1-4.md](docs/verification/phases-1-4.md).


