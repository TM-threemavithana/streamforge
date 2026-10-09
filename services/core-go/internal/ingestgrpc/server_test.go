package ingestgrpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ingestv1 "github.com/example/streamforge/services/core-go/gen/streamforge/ingest/v1"
	"github.com/example/streamforge/services/core-go/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type stubRepository struct{}

func (stubRepository) ProcessEvent(_ context.Context, event domain.TripEvent) (domain.EventResult, error) {
	return domain.EventResult{EventID: event.EventID, Outcome: domain.OutcomeAccepted, AckStage: "DATABASE_COMMITTED"}, nil
}

type failingRepository struct{ stubRepository }

func (failingRepository) ProcessEvent(context.Context, domain.TripEvent) (domain.EventResult, error) {
	return domain.EventResult{}, errors.New("pq: secret table detail")
}

func (stubRepository) ReportSourceRejection(_ context.Context, rejection domain.SourceRejection) (domain.EventResult, error) {
	return domain.EventResult{
		EventID: rejection.EventID, Outcome: domain.OutcomeRejected,
		AckStage: "DATABASE_COMMITTED", ReasonCode: rejection.ReasonCode,
	}, nil
}

func TestIngestBatchRejectsMoreThanMaximumRecords(t *testing.T) {
	server := NewServer(stubRepository{})
	request := &ingestv1.IngestBatchRequest{RequestId: "too-large"}
	request.Events = make([]*ingestv1.TripEvent, MaxBatchSize+1)
	_, err := server.IngestBatch(context.Background(), request)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("status = %v; want %v", status.Code(err), codes.ResourceExhausted)
	}
}

func TestReportSourceRejectionsReturnsCommittedOutcome(t *testing.T) {
	server := NewServer(stubRepository{})
	response, err := server.ReportSourceRejections(context.Background(), &ingestv1.SourceRejectionsRequest{
		RequestId: "rejection",
		Rejections: []*ingestv1.SourceRejection{{
			EventId:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			DatasetId: "dataset", RunId: "run", SourceRowNumber: 4,
			ReasonCode: "INVALID_PICKUP_ZONE", ValidationPolicyVersion: "v1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].Outcome != ingestv1.Outcome_OUTCOME_REJECTED || response.Results[0].AckStage != ingestv1.AckStage_ACK_STAGE_DATABASE_COMMITTED {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestUnexpectedRepositoryErrorsAreSanitized(t *testing.T) {
	server := NewServer(failingRepository{})
	request := &ingestv1.IngestBatchRequest{RequestId: "correlation-id", Events: []*ingestv1.TripEvent{{
		EventId:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		DatasetId: "dataset", RunId: "run", SourceSha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		PickupAt:     timestamppb.New(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
		DropoffAt:    timestamppb.New(time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)),
		PickupZoneId: 1,
	}}}
	_, err := server.IngestBatch(context.Background(), request)
	if status.Code(err) != codes.Unavailable || strings.Contains(status.Convert(err).Message(), "pq:") {
		t.Fatalf("unexpected status: %v", err)
	}
}

func TestProtocolResultKeepsKafkaAndDatabaseAcknowledgmentsDistinct(t *testing.T) {
	result := protocolResult(domain.EventResult{
		EventID: "event", Outcome: domain.OutcomeAccepted, AckStage: "KAFKA_PUBLISHED",
	})
	if result.AckStage != ingestv1.AckStage_ACK_STAGE_KAFKA_PUBLISHED {
		t.Fatalf("ack stage = %s; want KAFKA_PUBLISHED", result.AckStage)
	}
}
