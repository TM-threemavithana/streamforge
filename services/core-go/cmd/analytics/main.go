package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/example/streamforge/services/core-go/internal/eventstream"
	postgresrepo "github.com/example/streamforge/services/core-go/internal/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		slog.Error("analytics consumer stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := os.Getenv("STREAMFORGE_DATABASE_URL")
	if databaseURL == "" {
		return errors.New("STREAMFORGE_DATABASE_URL is required")
	}
	brokers := commaSeparated(os.Getenv("STREAMFORGE_KAFKA_BROKERS"))
	if len(brokers) == 0 {
		return errors.New("STREAMFORGE_KAFKA_BROKERS is required")
	}
	topic := valueOrDefault("STREAMFORGE_KAFKA_TOPIC", "streamforge.raw-events.v1")
	consumerGroup := valueOrDefault("STREAMFORGE_KAFKA_CONSUMER_GROUP", "streamforge-analytics-v1")

	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	readyContext, readyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer readyCancel()
	if err = database.PingContext(readyContext); err != nil {
		return err
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("streamforge-analytics-v1"),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer client.Close()
	if err = client.Ping(readyContext); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	consumer := eventstream.NewConsumer(
		client,
		consumerGroup,
		postgresrepo.NewEventRepository(database),
		postgresrepo.NewKafkaFailureRepository(database),
	)
	slog.Info("analytics consumer started", "brokers", brokers, "topic", topic, "consumer_group", consumerGroup)
	return consumer.Run(ctx)
}

func commaSeparated(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}

func valueOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
