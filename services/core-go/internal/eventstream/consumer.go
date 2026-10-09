package eventstream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/example/streamforge/services/core-go/internal/application"
	"github.com/example/streamforge/services/core-go/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumerFailure struct {
	ConsumerGroup string
	Topic         string
	Partition     int32
	Offset        int64
	MessageKey    string
	PayloadSHA256 string
	Reason        string
}

type FailureRecorder interface {
	RecordConsumerFailure(context.Context, ConsumerFailure) error
}

type Consumer struct {
	client        *kgo.Client
	consumerGroup string
	processor     application.EventRepository
	failures      FailureRecorder
}

func NewConsumer(client *kgo.Client, consumerGroup string, processor application.EventRepository, failures FailureRecorder) *Consumer {
	return &Consumer{client: client, consumerGroup: consumerGroup, processor: processor, failures: failures}
}

func (consumer *Consumer) Run(ctx context.Context) error {
	for {
		fetches := consumer.client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		for _, fetchError := range fetches.Errors() {
			// Broker and group-coordinator errors are commonly transient. franz-go
			// maintains the group session and retries internally, so continuing to
			// poll preserves the last committed offset without a false resolution.
			slog.Warn("Kafka poll error; record offsets remain unresolved", "topic", fetchError.Topic, "partition", fetchError.Partition, "error", fetchError.Err)
		}
		for iterator := fetches.RecordIter(); !iterator.Done(); {
			record := iterator.Next()
			if err := consumer.HandleRecord(ctx, record); err != nil {
				return err
			}
			if err := consumer.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit Kafka offset after database commit: %w", err)
			}
		}
	}
}

func (consumer *Consumer) HandleRecord(ctx context.Context, record *kgo.Record) error {
	envelope, err := Decode(record.Value)
	if err != nil {
		return consumer.recordPermanentFailure(ctx, record, err)
	}

	switch envelope.Kind {
	case KindTrip:
		_, err = consumer.processor.ProcessEvent(ctx, *envelope.Trip)
	case KindRejection:
		_, err = consumer.processor.ReportSourceRejection(ctx, *envelope.Rejection)
	default:
		err = fmt.Errorf("unsupported event kind %q", envelope.Kind)
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, domain.ErrInvalidInput) || errors.Is(err, domain.ErrRunNotRunning) {
		return consumer.recordPermanentFailure(ctx, record, err)
	}
	return fmt.Errorf("process Kafka record at %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, err)
}

func (consumer *Consumer) recordPermanentFailure(ctx context.Context, record *kgo.Record, processingError error) error {
	digest := sha256.Sum256(record.Value)
	failure := ConsumerFailure{
		ConsumerGroup: consumer.consumerGroup,
		Topic:         record.Topic, Partition: record.Partition, Offset: record.Offset,
		MessageKey: string(record.Key), PayloadSHA256: hex.EncodeToString(digest[:]),
		Reason: processingError.Error(),
	}
	if err := consumer.failures.RecordConsumerFailure(ctx, failure); err != nil {
		return fmt.Errorf("persist permanent Kafka failure: %w", err)
	}
	slog.Warn("Kafka record durably rejected", "consumer_group", failure.ConsumerGroup, "topic", failure.Topic, "partition", failure.Partition, "offset", failure.Offset, "reason", failure.Reason)
	return nil
}
