package eventstream_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
	"github.com/example/streamforge/services/core-go/internal/eventstream"
	postgresrepo "github.com/example/streamforge/services/core-go/internal/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerRestartBeforeAndAfterDatabaseCommitDoesNotDoubleCount(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	brokersValue := os.Getenv("STREAMFORGE_TEST_KAFKA_BROKERS")
	if databaseURL == "" || brokersValue == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL and STREAMFORGE_TEST_KAFKA_BROKERS to run Kafka crash integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := splitNonEmpty(brokersValue)
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	lockConnection, err := database.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConnection.Close()
	if _, err = lockConnection.ExecContext(ctx, `SELECT pg_advisory_lock(8291042024)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = lockConnection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(8291042024)`)
	}()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "streamforge-crash-test-" + suffix
	group := "streamforge-crash-test-" + suffix
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	if _, err = admin.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.DeleteTopic(cleanupContext, topic)
	})

	digest := sha256.Sum256([]byte(topic))
	sourceSHA := hex.EncodeToString(digest[:])
	var datasetID, runID string
	err = database.QueryRowContext(ctx, `
		INSERT INTO datasets (source_type, source_sha256, source_schema_version, filename, source_size_bytes)
		VALUES ('nyc-yellow', $1, 'v1', $2, 0) RETURNING id`, sourceSHA, topic+".parquet").Scan(&datasetID)
	if err != nil {
		t.Fatal(err)
	}
	if err = database.QueryRowContext(ctx, `INSERT INTO replay_runs (dataset_id, state, started_at) VALUES ($1, 'RUNNING', now()) RETURNING id`, datasetID).Scan(&runID); err != nil {
		t.Fatal(err)
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	repository := postgresrepo.NewEventRepository(database)
	failures := postgresrepo.NewKafkaFailureRepository(database)

	// Offset 0: the process receives the record and dies before starting the
	// database transaction. The restarted process must receive it again.
	first := crashTestEvent(datasetID, runID, sourceSHA, 0)
	produceCrashTestEvent(t, ctx, producer, topic, first)
	crashedClient, record := pollCrashTestRecord(t, ctx, brokers, topic, group)
	if record.Offset != 0 {
		t.Fatalf("first offset=%d", record.Offset)
	}
	crashedClient.Close()
	consumer := eventstream.NewConsumer(nil, group, repository, failures)
	processAndCommitCrashTestRecord(t, ctx, brokers, topic, group, record.Offset, consumer)

	// Offset 1: the database commits, then the process dies before committing
	// the Kafka offset. Replay after restart exercises database idempotency.
	second := crashTestEvent(datasetID, runID, sourceSHA, 1)
	produceCrashTestEvent(t, ctx, producer, topic, second)
	record = processWithoutCommitCrashTestRecord(t, ctx, brokers, topic, group, consumer)
	if record.Offset != 1 {
		t.Fatalf("second offset=%d", record.Offset)
	}
	processAndCommitCrashTestRecord(t, ctx, brokers, topic, group, record.Offset, consumer)
	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	committed, ok := offsets.Lookup(topic, 0)
	if !ok || committed.Err != nil || committed.At != 2 {
		t.Fatalf("committed offset=%+v present=%v", committed, ok)
	}

	var events, outcomes, trips int64
	if err = database.QueryRowContext(ctx, `SELECT count(*) FROM trip_events WHERE dataset_id = $1`, datasetID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err = database.QueryRowContext(ctx, `SELECT count(*) FROM run_event_outcomes WHERE run_id = $1`, runID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if err = database.QueryRowContext(ctx, `SELECT coalesce(sum(trip_count), 0) FROM hourly_zone_stats WHERE dataset_id = $1`, datasetID).Scan(&trips); err != nil {
		t.Fatal(err)
	}
	if events != 2 || outcomes != 2 || trips != 2 {
		t.Fatalf("restart double-counted: events=%d outcomes=%d aggregate trips=%d", events, outcomes, trips)
	}
}

func crashTestEvent(datasetID, runID, sourceSHA string, row uint64) domain.TripEvent {
	eventDigest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", sourceSHA, row)))
	fare := int64(1250)
	return domain.TripEvent{
		DatasetID: datasetID, RunID: runID, EventID: hex.EncodeToString(eventDigest[:]),
		SourceSHA256: sourceSHA, SourceRowNumber: row,
		PickupAt: time.Date(2024, 1, 2, 3, 0, 0, 0, time.UTC), DropoffAt: time.Date(2024, 1, 2, 3, 10, 0, 0, time.UTC),
		PickupZoneID: 10, DistanceMilliMiles: 1500, FareCents: &fare,
	}
}

func produceCrashTestEvent(t *testing.T, ctx context.Context, producer *kgo.Client, topic string, event domain.TripEvent) {
	t.Helper()
	payload, err := eventstream.EncodeTrip(event)
	if err != nil {
		t.Fatal(err)
	}
	result := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(event.EventID), Value: payload})[0]
	if result.Err != nil {
		t.Fatal(result.Err)
	}
}

func pollCrashTestRecord(t *testing.T, parent context.Context, brokers []string, topic, group string) (*kgo.Client, *kgo.Record) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	pollContext, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	for {
		fetches := client.PollRecords(pollContext, 1)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatal(errs[0])
		}
		iterator := fetches.RecordIter()
		if !iterator.Done() {
			record := iterator.Next()
			return client, record
		}
		if pollContext.Err() != nil {
			client.Close()
			t.Fatal(pollContext.Err())
		}
	}
}

func processWithoutCommitCrashTestRecord(t *testing.T, parent context.Context, brokers []string, topic, group string, consumer *eventstream.Consumer) *kgo.Record {
	t.Helper()
	client, record := pollCrashTestRecord(t, parent, brokers, topic, group)
	if err := consumer.HandleRecord(parent, record); err != nil {
		client.Close()
		t.Fatal(err)
	}
	client.Close()
	return record
}

func processAndCommitCrashTestRecord(t *testing.T, parent context.Context, brokers []string, topic, group string, expectedOffset int64, consumer *eventstream.Consumer) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pollContext, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	for {
		fetches := client.PollRecords(pollContext, 1)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatal(errs[0])
		}
		iterator := fetches.RecordIter()
		if iterator.Done() {
			if pollContext.Err() != nil {
				t.Fatal(pollContext.Err())
			}
			continue
		}
		record := iterator.Next()
		if record.Offset != expectedOffset {
			t.Fatalf("replayed offset=%d, expected=%d", record.Offset, expectedOffset)
		}
		if err = consumer.HandleRecord(pollContext, record); err != nil {
			t.Fatal(err)
		}
		if err = client.CommitRecords(pollContext, record); err != nil {
			t.Fatal(err)
		}
		return
	}
}

func splitNonEmpty(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}
