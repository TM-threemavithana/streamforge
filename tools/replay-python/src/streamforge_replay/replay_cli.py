from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from .replay import replay_parquet
from .zone_lookup import DEFAULT_ZONE_LOOKUP, load_approved_zones


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay historical NYC TLC Yellow Taxi rows to StreamForge over gRPC"
    )
    parser.add_argument("parquet_file")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--grpc-target", default="127.0.0.1:50051")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--zone-lookup", default=str(DEFAULT_ZONE_LOOKUP), help="checksum-pinned official TLC taxi-zone lookup CSV")
    args = parser.parse_args()
    zones = load_approved_zones(args.zone_lookup)
    report = replay_parquet(
        args.parquet_file,
        zones,
        args.dataset_id,
        args.run_id,
        args.grpc_target,
        batch_size=args.batch_size,
    )
    print(json.dumps(asdict(report), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
