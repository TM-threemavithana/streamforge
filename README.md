# StreamForge 2.0

StreamForge is a mobility operations and data-quality portfolio project. It replays the official NYC TLC Yellow Taxi January 2024 **historical** Parquet dataset; it is not live vehicle tracking.

The first milestone is deliberately small: a Python/PyArrow adapter that reads bounded record batches, normalizes the six authoritative source fields, derives reproducible source-row event IDs, and reports explicit validation failures. The eventual Go/PostgreSQL ingestion path will use database uniqueness and atomic aggregate updates so retries and crashes do not double-count accepted source events.

## Phase 1 contract

- Source identity: SHA-256 of the exact Parquet bytes.
- Event identity: SHA-256 of UTF-8 `nyc-yellow:v1:<lowercase-source-sha256>:<zero-based-logical-row>`.
- Source timestamps: naive values are New York wall time and become UTC. Ambiguous or nonexistent DST wall times are rejected unless future source metadata disambiguates them.
- Distance: integer thousandths of a mile, rounded half-up from decimal text.
- Fare: optional signed integer cents, rounded half-up. Negative fares are preserved for later anomaly policy.
- Zone validation: supplied approved lookup IDs. The CLI's `1:265` default is only an initial inspection convenience and will be replaced by the pinned official lookup.
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

1. Complete Phase 1 with the official file and pinned taxi-zone lookup.
2. Add the modular Go domain/application layer and PostgreSQL migrations with atomic idempotency tests.
3. Add versioned Protobuf/gRPC ingestion.
4. Add REST analytics and a read-only React/TypeScript dashboard.
5. Introduce Kafka, the independent Java rules service, Compose, Kubernetes/Helm, observability, security, and performance evidence only in their planned stages.

## Phase 2 status

The initial Go domain model, replay-run transition rules, persistence interface, six ordered PostgreSQL migrations, and transactional PostgreSQL event repository are present. The live PostgreSQL integration tests verify same-run retry stability, cross-run duplicate handling, exactly-once aggregate updates, and forced rollback with no partial event, outcome, or aggregate change.

```powershell
cd deploy\docker
docker compose up -d --wait postgres

cd ..\..\services\core-go
$env:STREAMFORGE_TEST_DATABASE_URL='postgres://streamforge:local-development-only@127.0.0.1:5433/streamforge?sslmode=disable'
go test ./... -count=1
```

Compose exposes the development database on loopback port `5433` by default to avoid colliding with a local PostgreSQL installation. Set `STREAMFORGE_POSTGRES_PORT` before starting Compose to choose another host port. If `docker compose up` reports that the Docker API pipe is missing on Windows, start Docker Desktop first. A valid Compose configuration is not evidence that the SQL migrations have executed; verify the live schema or run the integration tests against a newly initialized volume.


