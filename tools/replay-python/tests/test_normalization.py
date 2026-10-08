from datetime import datetime, timezone

import pytest

from streamforge_replay.model import CanonicalTrip, RejectedRow, RejectionCode
from streamforge_replay.normalization import normalize_row

SOURCE = "01" * 32
ZONES = frozenset({1, 2, 3})


def valid_row(**changes):
    row = {
        "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 30),
        "PULocationID": 1,
        "DOLocationID": 2,
        "trip_distance": 1.2345,
        "fare_amount": "12.345",
    }
    row.update(changes)
    return row


def test_valid_row_is_normalized_to_utc_and_fixed_point():
    result = normalize_row(valid_row(), SOURCE, 7, ZONES)
    assert isinstance(result, CanonicalTrip)
    assert result.pickup_at_utc == datetime(2024, 1, 15, 13, 0, tzinfo=timezone.utc)
    assert result.distance_milli_miles == 1235
    assert result.fare_cents == 1235


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"tpep_pickup_datetime": None}, RejectionCode.MISSING_PICKUP_TIMESTAMP),
        ({"tpep_dropoff_datetime": None}, RejectionCode.MISSING_DROPOFF_TIMESTAMP),
        ({"tpep_dropoff_datetime": datetime(2024, 1, 15, 7, 0)}, RejectionCode.DROPOFF_BEFORE_PICKUP),
        ({"PULocationID": 999}, RejectionCode.INVALID_PICKUP_ZONE),
        ({"DOLocationID": 999}, RejectionCode.INVALID_DROPOFF_ZONE),
        ({"trip_distance": -0.1}, RejectionCode.INVALID_DISTANCE),
        ({"trip_distance": float("nan")}, RejectionCode.INVALID_DISTANCE),
    ],
)
def test_invalid_rows_have_stable_reason_codes(changes, code):
    result = normalize_row(valid_row(**changes), SOURCE, 0, ZONES)
    assert isinstance(result, RejectedRow)
    assert result.reason_code is code


def test_negative_fare_is_preserved_but_missing_fare_is_allowed():
    negative = normalize_row(valid_row(fare_amount="-1.005"), SOURCE, 0, ZONES)
    missing = normalize_row(valid_row(fare_amount=None), SOURCE, 1, ZONES)
    assert isinstance(negative, CanonicalTrip) and negative.fare_cents == -101
    assert isinstance(missing, CanonicalTrip) and missing.fare_cents is None


@pytest.mark.parametrize(
    "wall_time,code",
    [
        (datetime(2024, 3, 10, 2, 30), RejectionCode.NONEXISTENT_LOCAL_TIMESTAMP),
        (datetime(2024, 11, 3, 1, 30), RejectionCode.AMBIGUOUS_LOCAL_TIMESTAMP),
    ],
)
def test_dst_edges_are_rejected_without_disambiguating_metadata(wall_time, code):
    result = normalize_row(valid_row(tpep_pickup_datetime=wall_time), SOURCE, 0, ZONES)
    assert isinstance(result, RejectedRow)
    assert result.reason_code is code

