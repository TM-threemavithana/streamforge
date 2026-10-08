from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class RejectionCode(StrEnum):
    MISSING_PICKUP_TIMESTAMP = "MISSING_PICKUP_TIMESTAMP"
    MISSING_DROPOFF_TIMESTAMP = "MISSING_DROPOFF_TIMESTAMP"
    AMBIGUOUS_LOCAL_TIMESTAMP = "AMBIGUOUS_LOCAL_TIMESTAMP"
    NONEXISTENT_LOCAL_TIMESTAMP = "NONEXISTENT_LOCAL_TIMESTAMP"
    DROPOFF_BEFORE_PICKUP = "DROPOFF_BEFORE_PICKUP"
    INVALID_PICKUP_ZONE = "INVALID_PICKUP_ZONE"
    INVALID_DROPOFF_ZONE = "INVALID_DROPOFF_ZONE"
    INVALID_DISTANCE = "INVALID_DISTANCE"
    INVALID_FARE = "INVALID_FARE"
    UNUSABLE_VALUE = "UNUSABLE_VALUE"


@dataclass(frozen=True, slots=True)
class CanonicalTrip:
    event_id: str
    source_sha256: str
    source_row_number: int
    pickup_at_utc: datetime
    dropoff_at_utc: datetime
    pickup_zone_id: int
    dropoff_zone_id: int | None
    distance_milli_miles: int
    fare_cents: int | None


@dataclass(frozen=True, slots=True)
class RejectedRow:
    event_id: str
    source_row_number: int
    reason_code: RejectionCode
    detail: str
    validation_policy_version: str = "nyc-yellow-validation:v1"

