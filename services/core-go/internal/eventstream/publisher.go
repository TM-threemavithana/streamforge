package eventstream

import (
	"context"
	"fmt"

	"github.com/example/streamforge/services/core-go/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

const KafkaPublished = "KAFKA_PUBLISHED"

type RecordPublisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
}

type BatchRecordPublisher interface {
	RecordPublisher
	PublishBatch(ctx context.Context, records []*kgo.Record) error
}

type KafkaPublisher struct {
	client *kgo.Client
}

func NewKafkaPublisher(client *kgo.Client) *KafkaPublisher {
	return &KafkaPublisher{client: client}
}

func (publisher *KafkaPublisher) Publish(ctx context.Context, topic string, key, value []byte) error {
	result := publisher.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value})
	if err := result.FirstErr(); err != nil {
		return fmt.Errorf("publish Kafka record: %w", err)
	}
	return nil
}

func (publisher *KafkaPublisher) PublishBatch(ctx context.Context, records []*kgo.Record) error {
	results := publisher.client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish Kafka batch: %w", err)
	}
	return nil
}

type Repository struct {
	topic     string
	publisher RecordPublisher
}

func NewRepository(topic string, publisher RecordPublisher) *Repository {
	return &Repository{topic: topic, publisher: publisher}
}

func (repository *Repository) ProcessEvent(ctx context.Context, event domain.TripEvent) (domain.EventResult, error) {
	if repository.topic == "" || event.EventID == "" || event.DatasetID == "" || event.RunID == "" {
		return domain.EventResult{}, fmt.Errorf("%w: topic, event ID, dataset ID, and run ID are required", domain.ErrInvalidInput)
	}
	payload, err := EncodeTrip(event)
	if err != nil {
		return domain.EventResult{}, fmt.Errorf("encode trip event: %w", err)
	}
	if err = repository.publisher.Publish(ctx, repository.topic, []byte(event.EventID), payload); err != nil {
		return domain.EventResult{}, err
	}
	return domain.EventResult{EventID: event.EventID, Outcome: domain.OutcomeAccepted, AckStage: KafkaPublished}, nil
}

func (repository *Repository) ReportSourceRejection(ctx context.Context, rejection domain.SourceRejection) (domain.EventResult, error) {
	if repository.topic == "" || rejection.EventID == "" || rejection.DatasetID == "" || rejection.RunID == "" {
		return domain.EventResult{}, fmt.Errorf("%w: topic, event ID, dataset ID, and run ID are required", domain.ErrInvalidInput)
	}
	payload, err := EncodeRejection(rejection)
	if err != nil {
		return domain.EventResult{}, fmt.Errorf("encode source rejection: %w", err)
	}
	if err = repository.publisher.Publish(ctx, repository.topic, []byte(rejection.EventID), payload); err != nil {
		return domain.EventResult{}, err
	}
	return domain.EventResult{
		EventID: rejection.EventID, Outcome: domain.OutcomeRejected,
		AckStage: KafkaPublished, ReasonCode: rejection.ReasonCode,
	}, nil
}

func (repository *Repository) ProcessBatch(ctx context.Context, events []domain.TripEvent) ([]domain.EventResult, error) {
	if len(events) == 0 {
		return nil, nil
	}
	records := make([]*kgo.Record, len(events))
	results := make([]domain.EventResult, len(events))
	for i, event := range events {
		if repository.topic == "" || event.EventID == "" || event.DatasetID == "" || event.RunID == "" {
			return nil, fmt.Errorf("%w: topic, event ID, dataset ID, and run ID are required", domain.ErrInvalidInput)
		}
		payload, err := EncodeTrip(event)
		if err != nil {
			return nil, fmt.Errorf("encode trip event: %w", err)
		}
		records[i] = &kgo.Record{
			Topic: repository.topic,
			Key:   []byte(event.EventID),
			Value: payload,
		}
		results[i] = domain.EventResult{
			EventID:  event.EventID,
			Outcome:  domain.OutcomeAccepted,
			AckStage: KafkaPublished,
		}
	}

	if batchPub, ok := repository.publisher.(BatchRecordPublisher); ok {
		if err := batchPub.PublishBatch(ctx, records); err != nil {
			return nil, err
		}
	} else {
		for _, rec := range records {
			if err := repository.publisher.Publish(ctx, rec.Topic, rec.Key, rec.Value); err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}
