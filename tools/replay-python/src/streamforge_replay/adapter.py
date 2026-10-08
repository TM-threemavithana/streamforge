from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import tempfile
from typing import Iterator

import pyarrow.parquet as pq

from .model import CanonicalTrip, RejectedRow
from .normalization import normalize_row

REQUIRED_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
)


@dataclass(frozen=True, slots=True)
class AdapterReport:
    source_sha256: str
    total_rows: int
    accepted_rows: int
    rejected_rows: int
    rejection_counts: dict[str, int]


def iter_normalized(
    path: str | Path, approved_zones: frozenset[int], batch_size: int = 500
) -> Iterator[CanonicalTrip | RejectedRow]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    with _source_snapshot(path) as (snapshot, checksum):
        yield from _iter_snapshot(snapshot, checksum, approved_zones, batch_size)


def _iter_snapshot(
    snapshot: Path,
    checksum: str,
    approved_zones: frozenset[int],
    batch_size: int,
) -> Iterator[CanonicalTrip | RejectedRow]:
    parquet = pq.ParquetFile(snapshot)
    try:
        missing = sorted(set(REQUIRED_COLUMNS) - set(parquet.schema_arrow.names))
        if missing:
            raise ValueError(f"unsupported Parquet schema; missing columns: {', '.join(missing)}")
        row_number = 0
        for batch in parquet.iter_batches(batch_size=batch_size, columns=list(REQUIRED_COLUMNS)):
            for row in batch.to_pylist():
                yield normalize_row(row, checksum, row_number, approved_zones)
                row_number += 1
    finally:
        parquet.close()


def inspect_parquet(
    path: str | Path, approved_zones: frozenset[int], batch_size: int = 500
) -> AdapterReport:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    counts: Counter[str] = Counter()
    accepted = 0
    total = 0
    with _source_snapshot(path) as (snapshot, checksum):
        for outcome in _iter_snapshot(snapshot, checksum, approved_zones, batch_size):
            total += 1
            if isinstance(outcome, CanonicalTrip):
                accepted += 1
            else:
                counts[outcome.reason_code.value] += 1
    return AdapterReport(checksum, total, accepted, total - accepted, dict(sorted(counts.items())))


@contextmanager
def _source_snapshot(path: str | Path) -> Iterator[tuple[Path, str]]:
    """Copy and hash once so replay bytes cannot diverge from source identity."""
    source_path = Path(path)
    digest = hashlib.sha256()
    snapshot_directory = os.environ.get("STREAMFORGE_SNAPSHOT_DIR")
    with tempfile.TemporaryDirectory(prefix="streamforge-source-", dir=snapshot_directory) as directory:
        snapshot = Path(directory) / source_path.name
        with source_path.open("rb") as source, snapshot.open("xb") as destination:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
                destination.write(chunk)
            destination.flush()
            os.fsync(destination.fileno())
        yield snapshot, digest.hexdigest()

