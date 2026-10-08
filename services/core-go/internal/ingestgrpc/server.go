package ingestgrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	ingestv1 "github.com/example/streamforge/services/core-go/gen/streamforge/ingest/v1"
	"github.com/example/streamforge/services/core-go/internal/application"
	"github.com/example/streamforge/services/core-go/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	MaxBatchSize    = 500
	MaxRequestBytes = 4 << 20
)

type Server struct {
	ingestv1.UnimplementedTripIngestionServiceServer
	repository application.EventRepository
}

func NewServer(repository application.EventRepository) *Server {
	return &Server{repository: repository}
}

func (server *Server) IngestBatch(ctx context.Context, request *ingestv1.IngestBatchRequest) (*ingestv1.IngestBatchResponse, error) {
	if err := validateBatch(request.GetRequestId(), len(request.GetEvents()), proto.Size(request)); err != nil {
		return nil, err
	}

	response := &ingestv1.IngestBatchResponse{RequestId: request.GetRequestId()}
	response.Results = make([]*ingestv1.EventResult, 0, len(request.GetEvents()))
	for index, message := range request.GetEvents() {
		event, err := domainEvent(message)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "events[%d]: %v", index, err)
		}
		result, err := server.repository.ProcessEvent(ctx, event)
		if err != nil {
			logRepositoryError(request.GetRequestId(), err)
			return nil, repositoryStatus(err)
		}
		response.Results = append(response.Results, protocolResult(result))
	}
	return response, nil
}

func (server *Server) ReportSourceRejections(ctx context.Context, request *ingestv1.SourceRejectionsRequest) (*ingestv1.SourceRejectionsResponse, error) {
	if err := validateBatch(request.GetRequestId(), len(request.GetRejections()), proto.Size(request)); err != nil {
		return nil, err
	}

	response := &ingestv1.SourceRejectionsResponse{RequestId: request.GetRequestId()}
	response.Results = make([]*ingestv1.EventResult, 0, len(request.GetRejections()))
	for index, message := range request.GetRejections() {
		if message == nil {
			return nil, status.Errorf(codes.InvalidArgument, "rejections[%d] is required", index)
		}
		result, err := server.repository.ReportSourceRejection(ctx, domain.SourceRejection{
			DatasetID:               message.GetDatasetId(),
			RunID:                   message.GetRunId(),
			EventID:                 message.GetEventId(),
			SourceRowNumber:         message.GetSourceRowNumber(),
			ReasonCode:              message.GetReasonCode(),
			Detail:                  message.GetDetail(),
			ValidationPolicyVersion: message.GetValidationPolicyVersion(),
		})
		if err != nil {
			logRepositoryError(request.GetRequestId(), err)
			return nil, repositoryStatus(err)
		}
		response.Results = append(response.Results, protocolResult(result))
	}
	return response, nil
}

func validateBatch(requestID string, size, requestBytes int) error {
	if requestID == "" {
		return status.Error(codes.InvalidArgument, "request_id is required")
	}
	if size == 0 {
		return status.Error(codes.InvalidArgument, "batch must contain at least one record")
	}
	if size > MaxBatchSize {
		return status.Errorf(codes.ResourceExhausted, "batch contains %d records; maximum is %d", size, MaxBatchSize)
	}
	if requestBytes > MaxRequestBytes {
		return status.Errorf(codes.ResourceExhausted, "request is %d bytes; maximum is %d", requestBytes, MaxRequestBytes)
	}
	return nil
}

func domainEvent(message *ingestv1.TripEvent) (domain.TripEvent, error) {
	if message == nil {
		return domain.TripEvent{}, errors.New("event is required")
	}
	if len(message.GetSourceSha256()) != 64 {
		return domain.TripEvent{}, errors.New("source_sha256 must contain 64 characters")
	}
	if message.GetPickupAt() == nil {
		return domain.TripEvent{}, errors.New("pickup_at is required")
	}
	if err := message.GetPickupAt().CheckValid(); err != nil {
		return domain.TripEvent{}, fmt.Errorf("pickup_at: %w", err)
	}
	if message.GetDropoffAt() == nil {
		return domain.TripEvent{}, errors.New("dropoff_at is required")
	}
	if err := message.GetDropoffAt().CheckValid(); err != nil {
		return domain.TripEvent{}, fmt.Errorf("dropoff_at: %w", err)
	}

	var dropoffZoneID *int32
	if message.DropoffZoneId != nil {
		value := message.GetDropoffZoneId()
		dropoffZoneID = &value
	}
	var fareCents *int64
	if message.FareCents != nil {
		value := message.GetFareCents()
		fareCents = &value
	}
	return domain.TripEvent{
		DatasetID:          message.GetDatasetId(),
		RunID:              message.GetRunId(),
		EventID:            message.GetEventId(),
		SourceSHA256:       message.GetSourceSha256(),
		SourceRowNumber:    message.GetSourceRowNumber(),
		PickupAt:           message.GetPickupAt().AsTime(),
		DropoffAt:          message.GetDropoffAt().AsTime(),
		PickupZoneID:       message.GetPickupZoneId(),
		DropoffZoneID:      dropoffZoneID,
		DistanceMilliMiles: message.GetDistanceMilliMiles(),
		FareCents:          fareCents,
	}, nil
}

func repositoryStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrRunNotRunning):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Unavailable, "storage operation failed; retry with the same request and event IDs")
	}
}

func logRepositoryError(requestID string, err error) {
	if errors.Is(err, domain.ErrInvalidInput) || errors.Is(err, domain.ErrRunNotRunning) {
		return
	}
	slog.Error("gRPC repository operation failed", "request_id", requestID, "error", err)
}

func protocolResult(result domain.EventResult) *ingestv1.EventResult {
	outcome := ingestv1.Outcome_OUTCOME_UNSPECIFIED
	switch result.Outcome {
	case domain.OutcomeAccepted:
		outcome = ingestv1.Outcome_OUTCOME_ACCEPTED
	case domain.OutcomeDuplicate:
		outcome = ingestv1.Outcome_OUTCOME_DUPLICATE
	case domain.OutcomeRejected:
		outcome = ingestv1.Outcome_OUTCOME_REJECTED
	}
	ackStage := ingestv1.AckStage_ACK_STAGE_UNSPECIFIED
	if result.AckStage == "DATABASE_COMMITTED" {
		ackStage = ingestv1.AckStage_ACK_STAGE_DATABASE_COMMITTED
	}
	return &ingestv1.EventResult{
		EventId: result.EventID, Outcome: outcome, AckStage: ackStage,
		ReasonCode: result.ReasonCode,
	}
}
