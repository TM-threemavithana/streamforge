from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: create_demo_fixture.py OUTPUT.parquet")
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    start = datetime(2024, 1, 8, 8, 0)
    pickups = [start + timedelta(minutes=index) for index in range(100)]
    table = pa.table(
        {
            "tpep_pickup_datetime": pickups,
            "tpep_dropoff_datetime": [value + timedelta(minutes=10) for value in pickups],
            "PULocationID": [999 if index % 20 == 0 else 10 + index % 3 for index in range(100)],
            "DOLocationID": [20 for _ in range(100)],
            "trip_distance": [-1.0 if index % 20 == 1 else 2.5 for index in range(100)],
            "fare_amount": ["15.25" for _ in range(100)],
        }
    )
    pq.write_table(table, output, row_group_size=17)
    print(output)


if __name__ == "__main__":
    main()
