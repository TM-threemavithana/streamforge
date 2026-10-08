from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any
from zoneinfo import ZoneInfo

from .identity import event_id
from .model import CanonicalTrip, RejectedRow, RejectionCode

NYC = ZoneInfo("America/New_York")


class RowRejected(ValueError):
    def __init__(self, code: RejectionCode, detail: str):
        super().__init__(detail)
        self.code = code


def _strict_local_to_utc(value: Any, missing_code: RejectionCode) -> datetime:
    if value is None:
        raise RowRejected(missing_code, "timestamp is required")
    if not isinstance(value, datetime):
        raise RowRejected(RejectionCode.UNUSABLE_VALUE, "timestamp is not a datetime")
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc)

    candidates: list[datetime] = []
    for fold in (0, 1):
        local = value.replace(tzinfo=NYC, fold=fold)
        utc = local.astimezone(timezone.utc)
        if utc.astimezone(NYC).replace(tzinfo=None) == value:
            candidates.append(utc)
    unique = {candidate for candidate in candidates}
    if not unique:
        raise RowRejected(RejectionCode.NONEXISTENT_LOCAL_TIMESTAMP, f"nonexistent New York wall time: {value.isoformat()}")
    if len(unique) > 1:
        raise RowRejected(RejectionCode.AMBIGUOUS_LOCAL_TIMESTAMP, f"ambiguous New York wall time: {value.isoformat()}")
    return unique.pop()


def _fixed_point(value: Any, scale: int, code: RejectionCode, *, non_negative: bool) -> int:
    if value is None:
        raise RowRejected(code, "value is required")
    if isinstance(value, float) and not math.isfinite(value):
        raise RowRejected(code, "value must be finite")
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise RowRejected(code, "value is not numeric") from None
    if not decimal_value.is_finite() or (non_negative and decimal_value < 0):
        raise RowRejected(code, "value is outside the accepted domain")
    return int((decimal_value * scale).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _zone(value: Any, approved_zones: frozenset[int], *, required: bool) -> int | None:
    if value is None and not required:
        return None
    code = RejectionCode.INVALID_PICKUP_ZONE if required else RejectionCode.INVALID_DROPOFF_ZONE
    try:
        zone = int(value)
    except (TypeError, ValueError, OverflowError):
        raise RowRejected(code, "zone is not an integer") from None
    if zone not in approved_zones:
        raise RowRejected(code, f"zone {zone} is not in the approved lookup")
    return zone


def normalize_row(
    row: dict[str, Any], source_sha256: str, row_number: int, approved_zones: frozenset[int]
) -> CanonicalTrip | RejectedRow:
    stable_id = event_id(source_sha256, row_number)
    try:
        pickup = _strict_local_to_utc(row.get("tpep_pickup_datetime"), RejectionCode.MISSING_PICKUP_TIMESTAMP)
        dropoff = _strict_local_to_utc(row.get("tpep_dropoff_datetime"), RejectionCode.MISSING_DROPOFF_TIMESTAMP)
        if dropoff < pickup:
            raise RowRejected(RejectionCode.DROPOFF_BEFORE_PICKUP, "dropoff precedes pickup")
        pickup_zone = _zone(row.get("PULocationID"), approved_zones, required=True)
        dropoff_zone = _zone(row.get("DOLocationID"), approved_zones, required=False)
        distance = _fixed_point(row.get("trip_distance"), 1000, RejectionCode.INVALID_DISTANCE, non_negative=True)
        fare_value = row.get("fare_amount")
        fare = None if fare_value is None else _fixed_point(fare_value, 100, RejectionCode.INVALID_FARE, non_negative=False)
        return CanonicalTrip(
            event_id=stable_id,
            source_sha256=source_sha256.lower(),
            source_row_number=row_number,
            pickup_at_utc=pickup,
            dropoff_at_utc=dropoff,
            pickup_zone_id=pickup_zone,  # type: ignore[arg-type]
            dropoff_zone_id=dropoff_zone,
            distance_milli_miles=distance,
            fare_cents=fare,
        )
    except RowRejected as error:
        return RejectedRow(stable_id, row_number, error.code, str(error))

