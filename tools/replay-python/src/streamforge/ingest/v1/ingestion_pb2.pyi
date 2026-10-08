from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Outcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTCOME_UNSPECIFIED: _ClassVar[Outcome]
    OUTCOME_ACCEPTED: _ClassVar[Outcome]
    OUTCOME_DUPLICATE: _ClassVar[Outcome]
    OUTCOME_REJECTED: _ClassVar[Outcome]

class AckStage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACK_STAGE_UNSPECIFIED: _ClassVar[AckStage]
    ACK_STAGE_DATABASE_COMMITTED: _ClassVar[AckStage]
    ACK_STAGE_KAFKA_PUBLISHED: _ClassVar[AckStage]
OUTCOME_UNSPECIFIED: Outcome
OUTCOME_ACCEPTED: Outcome
OUTCOME_DUPLICATE: Outcome
OUTCOME_REJECTED: Outcome
ACK_STAGE_UNSPECIFIED: AckStage
ACK_STAGE_DATABASE_COMMITTED: AckStage
ACK_STAGE_KAFKA_PUBLISHED: AckStage

class TripEvent(_message.Message):
    __slots__ = ("event_id", "dataset_id", "run_id", "source_sha256", "source_row_number", "pickup_at", "dropoff_at", "pickup_zone_id", "dropoff_zone_id", "distance_milli_miles", "fare_cents")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    DATASET_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_SHA256_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ROW_NUMBER_FIELD_NUMBER: _ClassVar[int]
    PICKUP_AT_FIELD_NUMBER: _ClassVar[int]
    DROPOFF_AT_FIELD_NUMBER: _ClassVar[int]
    PICKUP_ZONE_ID_FIELD_NUMBER: _ClassVar[int]
    DROPOFF_ZONE_ID_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_MILLI_MILES_FIELD_NUMBER: _ClassVar[int]
    FARE_CENTS_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    dataset_id: str
    run_id: str
    source_sha256: str
    source_row_number: int
    pickup_at: _timestamp_pb2.Timestamp
    dropoff_at: _timestamp_pb2.Timestamp
    pickup_zone_id: int
    dropoff_zone_id: int
    distance_milli_miles: int
    fare_cents: int
    def __init__(self, event_id: _Optional[str] = ..., dataset_id: _Optional[str] = ..., run_id: _Optional[str] = ..., source_sha256: _Optional[str] = ..., source_row_number: _Optional[int] = ..., pickup_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., dropoff_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., pickup_zone_id: _Optional[int] = ..., dropoff_zone_id: _Optional[int] = ..., distance_milli_miles: _Optional[int] = ..., fare_cents: _Optional[int] = ...) -> None: ...

class IngestBatchRequest(_message.Message):
    __slots__ = ("request_id", "events")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    EVENTS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    events: _containers.RepeatedCompositeFieldContainer[TripEvent]
    def __init__(self, request_id: _Optional[str] = ..., events: _Optional[_Iterable[_Union[TripEvent, _Mapping]]] = ...) -> None: ...

class IngestBatchResponse(_message.Message):
    __slots__ = ("request_id", "results")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    results: _containers.RepeatedCompositeFieldContainer[EventResult]
    def __init__(self, request_id: _Optional[str] = ..., results: _Optional[_Iterable[_Union[EventResult, _Mapping]]] = ...) -> None: ...

class SourceRejection(_message.Message):
    __slots__ = ("event_id", "dataset_id", "run_id", "source_row_number", "reason_code", "detail", "validation_policy_version")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    DATASET_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ROW_NUMBER_FIELD_NUMBER: _ClassVar[int]
    REASON_CODE_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    VALIDATION_POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    dataset_id: str
    run_id: str
    source_row_number: int
    reason_code: str
    detail: str
    validation_policy_version: str
    def __init__(self, event_id: _Optional[str] = ..., dataset_id: _Optional[str] = ..., run_id: _Optional[str] = ..., source_row_number: _Optional[int] = ..., reason_code: _Optional[str] = ..., detail: _Optional[str] = ..., validation_policy_version: _Optional[str] = ...) -> None: ...

class SourceRejectionsRequest(_message.Message):
    __slots__ = ("request_id", "rejections")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    REJECTIONS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    rejections: _containers.RepeatedCompositeFieldContainer[SourceRejection]
    def __init__(self, request_id: _Optional[str] = ..., rejections: _Optional[_Iterable[_Union[SourceRejection, _Mapping]]] = ...) -> None: ...

class SourceRejectionsResponse(_message.Message):
    __slots__ = ("request_id", "results")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    results: _containers.RepeatedCompositeFieldContainer[EventResult]
    def __init__(self, request_id: _Optional[str] = ..., results: _Optional[_Iterable[_Union[EventResult, _Mapping]]] = ...) -> None: ...

class EventResult(_message.Message):
    __slots__ = ("event_id", "outcome", "ack_stage", "reason_code")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    ACK_STAGE_FIELD_NUMBER: _ClassVar[int]
    REASON_CODE_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    outcome: Outcome
    ack_stage: AckStage
    reason_code: str
    def __init__(self, event_id: _Optional[str] = ..., outcome: _Optional[_Union[Outcome, str]] = ..., ack_stage: _Optional[_Union[AckStage, str]] = ..., reason_code: _Optional[str] = ...) -> None: ...
