package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/example/streamforge/services/core-go/internal/eventstream"
)

type KafkaFailureRepository struct {
	database *sql.DB
}

func NewKafkaFailureRepository(database *sql.DB) *KafkaFailureRepository {
	return &KafkaFailureRepository{database: database}
}

func (repository *KafkaFailureRepository) RecordConsumerFailure(ctx context.Context, failure eventstream.ConsumerFailure) error {
	_, err := repository.database.ExecContext(ctx, `
		INSERT INTO kafka_consumer_failures (
			consumer_group, topic, partition_id, offset_id, message_key,
			payload_sha256, reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (consumer_group, topic, partition_id, offset_id) DO NOTHING`,
		failure.ConsumerGroup, failure.Topic, failure.Partition, failure.Offset,
		failure.MessageKey, failure.PayloadSHA256, failure.Reason,
	)
	if err != nil {
		return fmt.Errorf("insert Kafka consumer failure: %w", err)
	}
	return nil
}
