from __future__ import annotations

import csv
from pathlib import Path

from .identity import file_sha256

PINNED_ZONE_LOOKUP_SHA256 = "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed"
DEFAULT_ZONE_LOOKUP = Path(__file__).resolve().parents[4] / "data" / "reference" / "taxi_zone_lookup.csv"
REQUIRED_HEADERS = frozenset({"LocationID", "Borough", "Zone", "service_zone"})


def load_approved_zones(
    path: str | Path = DEFAULT_ZONE_LOOKUP,
    *,
    expected_sha256: str = PINNED_ZONE_LOOKUP_SHA256,
) -> frozenset[int]:
    lookup = Path(path)
    actual_sha256 = file_sha256(lookup)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"taxi-zone lookup checksum mismatch: expected {expected_sha256}, got {actual_sha256}"
        )

    with lookup.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        headers = frozenset(reader.fieldnames or ())
        if not REQUIRED_HEADERS.issubset(headers):
            missing = ", ".join(sorted(REQUIRED_HEADERS - headers))
            raise ValueError(f"taxi-zone lookup is missing columns: {missing}")
        zones: set[int] = set()
        for row_number, row in enumerate(reader, start=2):
            try:
                location_id = int(row["LocationID"])
            except (TypeError, ValueError) as error:
                raise ValueError(f"invalid LocationID at lookup row {row_number}") from error
            if location_id < 1 or location_id in zones:
                raise ValueError(f"invalid or duplicate LocationID {location_id} at lookup row {row_number}")
            zones.add(location_id)
    if not zones:
        raise ValueError("taxi-zone lookup is empty")
    return frozenset(zones)
