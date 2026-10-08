from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from streamforge_replay.grpc_client import IngestionClient, RetryPolicy
from streamforge_replay.model import CanonicalTrip


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--event-id", required=True)
    parser.add_argument("--source-sha256", required=True)
    args = parser.parse_args()

    pickup = datetime(2024, 1, 3, 14, 15, tzinfo=timezone.utc)
    event = CanonicalTrip(
        event_id=args.event_id,
        source_sha256=args.source_sha256,
        source_row_number=0,
        pickup_at_utc=pickup,
        dropoff_at_utc=pickup + timedelta(minutes=12),
        pickup_zone_id=10,
        dropoff_zone_id=20,
        distance_milli_miles=3200,
        fare_cents=1450,
    )
    policy = RetryPolicy(
        max_attempts=3,
        initial_backoff_seconds=0.01,
        maximum_backoff_seconds=0.02,
        jitter_ratio=0,
    )
    with IngestionClient(args.target, timeout_seconds=5, retry_policy=policy) as client:
        results = client.ingest_batch(
            args.dataset_id,
            args.run_id,
            [event],
            request_id="python-response-loss-probe",
        )
    print(json.dumps([asdict(result) for result in results], sort_keys=True))


if __name__ == "__main__":
    main()
