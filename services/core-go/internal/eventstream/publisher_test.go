package eventstream

import (
	"context"
	"testing"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
)

type capturedRecord struct {
	topic string
	key   []byte
	value []byte
}

type capturePublisher struct {
	records []capturedRecord
	err     error
}

func (publisher *capturePublisher) Publish(_ context.Context, topic string, key, value []byte) error {
	publisher.records = append(publisher.records, capturedRecord{topic: topic, key: key, value: value})
	return publisher.err
}

func TestRepositoryPublishesStableEventKeyAndKafkaAck(t *testing.T) {
	capture := &capturePublisher{}
	repository := NewRepository("streamforge.raw-events.v1", capture)
	event := domain.TripEvent{
		DatasetID: "dataset", RunID: "run", EventID: "event-id", SourceSHA256: "source",
		PickupAt: time.Date(2024, 1, 1, 1, 2, 3, 0, time.UTC), DropoffAt: time.Date(2024, 1, 1, 1, 3, 3, 0, time.UTC),
		PickupZoneID: 10, DistanceMilliMiles: 1000,
	}
	result, err := repository.ProcessEvent(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if result.AckStage != KafkaPublished || result.Outcome != domain.OutcomeAccepted {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(capture.records) != 1 || capture.records[0].topic != "streamforge.raw-events.v1" || string(capture.records[0].key) != event.EventID {
		t.Fatalf("unexpected record: %+v", capture.records)
	}
	envelope, err := Decode(capture.records[0].value)
	if err != nil || envelope.Trip == nil || envelope.Trip.EventID != event.EventID {
		t.Fatalf("unexpected envelope: %+v, %v", envelope, err)
	}
}

func TestDecodeRejectsUnknownSchema(t *testing.T) {
	if _, err := Decode([]byte(`{"schema_version":"v999","kind":"TRIP","trip":{}}`)); err == nil {
		t.Fatal("expected unsupported schema to fail")
	}
}
