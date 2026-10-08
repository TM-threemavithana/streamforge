from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from streamforge_replay.adapter import inspect_parquet, iter_normalized
from streamforge_replay.model import CanonicalTrip


def write_fixture(path):
    table = pa.table(
        {
            "tpep_pickup_datetime": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 1, 0), datetime(2024, 1, 1, 2, 0)],
            "tpep_dropoff_datetime": [datetime(2024, 1, 1, 0, 10), datetime(2024, 1, 1, 1, 10), datetime(2024, 1, 1, 2, 10)],
            "PULocationID": [1, 999, 2],
            "DOLocationID": [2, 2, 1],
            "trip_distance": [1.0, 2.0, 3.0],
            "fare_amount": [10.0, 20.0, 30.0],
            "ignored_source_column": ["a", "b", "c"],
        }
    )
    pq.write_table(table, path, row_group_size=2)


def test_real_parquet_batches_keep_global_logical_row_positions(tmp_path):
    path = tmp_path / "fixture.parquet"
    write_fixture(path)
    outcomes = list(iter_normalized(path, frozenset({1, 2}), batch_size=1))
    assert [item.source_row_number for item in outcomes] == [0, 1, 2]
    assert len({item.event_id for item in outcomes}) == 3
    assert isinstance(outcomes[0], CanonicalTrip)
    report = inspect_parquet(path, frozenset({1, 2}), batch_size=2)
    assert (report.total_rows, report.accepted_rows, report.rejected_rows) == (3, 2, 1)
    assert report.rejection_counts == {"INVALID_PICKUP_ZONE": 1}


def test_missing_required_column_fails_once_at_file_level(tmp_path):
    path = tmp_path / "bad-schema.parquet"
    pq.write_table(pa.table({"PULocationID": [1]}), path)
    with pytest.raises(ValueError, match="missing columns"):
        list(iter_normalized(path, frozenset({1}), batch_size=10))

