from __future__ import annotations

import random
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from streamforge.ingest.v1 import ingestion_pb2, ingestion_pb2_grpc

from .model import CanonicalTrip, RejectedRow

MAX_BATCH_SIZE = 500
RETRYABLE_STATUS_CODES = frozenset(
    {
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.UNAVAILABLE,
    }
)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 4
    initial_backoff_seconds: float = 0.1
    maximum_backoff_seconds: float = 2.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if self.initial_backoff_seconds < 0 or self.maximum_backoff_seconds < 0:
            raise ValueError("retry backoff must be non-negative")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between zero and one")


@dataclass(frozen=True, slots=True)
class EventResult:
    event_id: str
    outcome: str
    ack_stage: str
    reason_code: str


class IngestionClient:
    def __init__(
        self,
        target: str,
        *,
        timeout_seconds: float = 30.0,
        retry_policy: RetryPolicy = RetryPolicy(),
        channel: grpc.Channel | None = None,
    ) -> None:
        self._channel = channel or grpc.insecure_channel(target)
        self._owns_channel = channel is None
        self._stub = ingestion_pb2_grpc.TripIngestionServiceStub(self._channel)
        self._timeout_seconds = timeout_seconds
        self._retry_policy = retry_policy

    def close(self) -> None:
        if self._owns_channel:
            self._channel.close()

    def __enter__(self) -> IngestionClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def ingest_batch(
        self,
        dataset_id: str,
        run_id: str,
        events: Iterable[CanonicalTrip],
        *,
        request_id: str | None = None,
    ) -> tuple[EventResult, ...]:
        event_list = list(events)
        _validate_batch_size(event_list)
        request = ingestion_pb2.IngestBatchRequest(
            request_id=request_id or str(uuid.uuid4()),
            events=[_trip_message(dataset_id, run_id, event) for event in event_list],
        )
        response = self._call_with_retry(self._stub.IngestBatch, request)
        return tuple(_result(item) for item in response.results)

    def report_source_rejections(
        self,
        dataset_id: str,
        run_id: str,
        rejections: Iterable[RejectedRow],
        *,
        request_id: str | None = None,
    ) -> tuple[EventResult, ...]:
        rejection_list = list(rejections)
        _validate_batch_size(rejection_list)
        request = ingestion_pb2.SourceRejectionsRequest(
            request_id=request_id or str(uuid.uuid4()),
            rejections=[
                ingestion_pb2.SourceRejection(
                    event_id=item.event_id,
                    dataset_id=dataset_id,
                    run_id=run_id,
                    source_row_number=item.source_row_number,
                    reason_code=item.reason_code.value,
                    detail=item.detail,
                    validation_policy_version=item.validation_policy_version,
                )
                for item in rejection_list
            ],
        )
        response = self._call_with_retry(self._stub.ReportSourceRejections, request)
        return tuple(_result(item) for item in response.results)

    def _call_with_retry(self, method: Callable[..., object], request: object) -> object:
        backoff = self._retry_policy.initial_backoff_seconds
        for attempt in range(1, self._retry_policy.max_attempts + 1):
            try:
                return method(request, timeout=self._timeout_seconds)
            except grpc.RpcError as error:
                if error.code() not in RETRYABLE_STATUS_CODES or attempt == self._retry_policy.max_attempts:
                    raise
                jitter = backoff * self._retry_policy.jitter_ratio
                time.sleep(max(0.0, backoff + random.uniform(-jitter, jitter)))
                backoff = min(backoff * 2, self._retry_policy.maximum_backoff_seconds)
        raise RuntimeError("retry loop ended without a response")


def _validate_batch_size(items: Sequence[object]) -> None:
    if not items:
        raise ValueError("batch must contain at least one record")
    if len(items) > MAX_BATCH_SIZE:
        raise ValueError(f"batch contains {len(items)} records; maximum is {MAX_BATCH_SIZE}")


def _trip_message(dataset_id: str, run_id: str, event: CanonicalTrip) -> ingestion_pb2.TripEvent:
    pickup_at = Timestamp()
    pickup_at.FromDatetime(event.pickup_at_utc)
    dropoff_at = Timestamp()
    dropoff_at.FromDatetime(event.dropoff_at_utc)
    fields: dict[str, object] = {
        "event_id": event.event_id,
        "dataset_id": dataset_id,
        "run_id": run_id,
        "source_sha256": event.source_sha256,
        "source_row_number": event.source_row_number,
        "pickup_at": pickup_at,
        "dropoff_at": dropoff_at,
        "pickup_zone_id": event.pickup_zone_id,
        "distance_milli_miles": event.distance_milli_miles,
    }
    if event.dropoff_zone_id is not None:
        fields["dropoff_zone_id"] = event.dropoff_zone_id
    if event.fare_cents is not None:
        fields["fare_cents"] = event.fare_cents
    return ingestion_pb2.TripEvent(**fields)


def _result(message: ingestion_pb2.EventResult) -> EventResult:
    return EventResult(
        event_id=message.event_id,
        outcome=ingestion_pb2.Outcome.Name(message.outcome),
        ack_stage=ingestion_pb2.AckStage.Name(message.ack_stage),
        reason_code=message.reason_code,
    )
