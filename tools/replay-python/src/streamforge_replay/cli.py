from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from .adapter import inspect_parquet


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect a historical NYC TLC Yellow Taxi Parquet file")
    parser.add_argument("parquet_file")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--zone-range",
        default="1:265",
        help="inclusive approved taxi-zone ID range for initial inspection (default: 1:265)",
    )
    args = parser.parse_args()
    start_text, end_text = args.zone_range.split(":", maxsplit=1)
    zones = frozenset(range(int(start_text), int(end_text) + 1))
    report = inspect_parquet(args.parquet_file, zones, args.batch_size)
    print(json.dumps(asdict(report), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

