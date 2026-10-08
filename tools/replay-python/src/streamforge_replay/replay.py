from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from .adapter import iter_normalized
from .grpc_client import IngestionClient, MAX_BATCH_SIZE
from .model import CanonicalTrip, RejectedRow


@dataclass(frozen=True, slots=True)
class ReplayReport:
    total_rows: int
    accepted_outcomes: int
    duplicate_outcomes: int
    rejected_outcomes: int


def replay_parquet(
    path: str | Path,
    approved_zones: frozenset[int],
    dataset_id: str,
    run_id: str,
    target: str,
    *,
    batch_size: int = MAX_BATCH_SIZE,
) -> ReplayReport:
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")

    counts: Counter[str] = Counter()
    total = 0
    accepted: list[CanonicalTrip] = []
    rejected: list[RejectedRow] = []

    with IngestionClient(target) as client:
        def flush() -> None:
            if accepted:
                counts.update(result.outcome for result in client.ingest_batch(dataset_id, run_id, accepted))
                accepted.clear()
            if rejected:
                counts.update(
                    result.outcome
                    for result in client.report_source_rejections(dataset_id, run_id, rejected)
                )
                rejected.clear()

        for item in iter_normalized(path, approved_zones, batch_size):
            total += 1
            if isinstance(item, CanonicalTrip):
                accepted.append(item)
            else:
                rejected.append(item)
            if len(accepted) + len(rejected) >= batch_size:
                flush()
        flush()

    return ReplayReport(
        total_rows=total,
        accepted_outcomes=counts["OUTCOME_ACCEPTED"],
        duplicate_outcomes=counts["OUTCOME_DUPLICATE"],
        rejected_outcomes=counts["OUTCOME_REJECTED"],
    )
