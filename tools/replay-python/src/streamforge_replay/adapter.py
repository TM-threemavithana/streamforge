from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pyarrow.parquet as pq

from .identity import file_sha256
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
    parquet = pq.ParquetFile(path)
    missing = sorted(set(REQUIRED_COLUMNS) - set(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"unsupported Parquet schema; missing columns: {', '.join(missing)}")
    checksum = file_sha256(path)
    row_number = 0
    for batch in parquet.iter_batches(batch_size=batch_size, columns=list(REQUIRED_COLUMNS)):
        for row in batch.to_pylist():
            yield normalize_row(row, checksum, row_number, approved_zones)
            row_number += 1


def inspect_parquet(
    path: str | Path, approved_zones: frozenset[int], batch_size: int = 500
) -> AdapterReport:
    counts: Counter[str] = Counter()
    accepted = 0
    total = 0
    checksum = file_sha256(path)
    for outcome in iter_normalized(path, approved_zones, batch_size):
        total += 1
        if isinstance(outcome, CanonicalTrip):
            accepted += 1
        else:
            counts[outcome.reason_code.value] += 1
    return AdapterReport(checksum, total, accepted, total - accepted, dict(sorted(counts.items())))

