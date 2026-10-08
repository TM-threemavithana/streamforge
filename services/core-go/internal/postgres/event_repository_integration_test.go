package postgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
	_ "github.com/lib/pq"
)

func TestProcessEventIsIdempotentAcrossRetriesAndRuns(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	database, err := sql.Open("postgres", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = database.ExecContext(ctx, `TRUNCATE run_event_outcomes, hourly_zone_stats, rejected_events, trip_events, replay_runs, datasets CASCADE`)
	if err != nil {
		t.Fatalf("clean database: %v", err)
	}

	var datasetID, firstRunID, secondRunID string
	err = database.QueryRowContext(ctx, `
		INSERT INTO datasets (source_type, source_sha256, source_schema_version, filename)
		VALUES ('nyc-yellow', $1, 'v1', 'fixture.parquet') RETURNING id`, "ab"+repeat("0", 62)).Scan(&datasetID)
	if err != nil {
		t.Fatal(err)
	}
	for _, destination := range []*string{&firstRunID, &secondRunID} {
		err = database.QueryRowContext(ctx, `INSERT INTO replay_runs (dataset_id, state, started_at) VALUES ($1, 'RUNNING', now()) RETURNING id`, datasetID).Scan(destination)
		if err != nil {
			t.Fatal(err)
		}
	}

	fare := int64(1234)
	event := domain.TripEvent{
		DatasetID: datasetID, RunID: firstRunID, EventID: repeat("1", 64), SourceRowNumber: 0,
		PickupAt:     time.Date(2024, 1, 1, 12, 15, 0, 0, time.UTC),
		DropoffAt:    time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC),
		PickupZoneID: 10, DistanceMilliMiles: 2500, FareCents: &fare,
	}
	repository := NewEventRepository(database)

	first, err := repository.ProcessEvent(ctx, event)
	if err != nil || first.Outcome != domain.OutcomeAccepted {
		t.Fatalf("first insert = %+v, %v", first, err)
	}
	retry, err := repository.ProcessEvent(ctx, event)
	if err != nil || retry.Outcome != domain.OutcomeAccepted {
		t.Fatalf("same-run retry = %+v, %v", retry, err)
	}
	event.RunID = secondRunID
	duplicate, err := repository.ProcessEvent(ctx, event)
	if err != nil || duplicate.Outcome != domain.OutcomeDuplicate {
		t.Fatalf("cross-run replay = %+v, %v", duplicate, err)
	}

	var trips, distance, fares, fareCount int64
	err = database.QueryRowContext(ctx, `SELECT trip_count, total_distance_milli_miles, total_fare_cents, fare_observed_count FROM hourly_zone_stats`).Scan(&trips, &distance, &fares, &fareCount)
	if err != nil {
		t.Fatal(err)
	}
	if trips != 1 || distance != 2500 || fares != 1234 || fareCount != 1 {
		t.Fatalf("aggregate changed after retry: %d %d %d %d", trips, distance, fares, fareCount)
	}
}

func TestProcessEventRollsBackWhenAggregateUpdateFails(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	database, err := sql.Open("postgres", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = database.ExecContext(ctx, `TRUNCATE run_event_outcomes, hourly_zone_stats, rejected_events, trip_events, replay_runs, datasets CASCADE`)
	if err != nil {
		t.Fatalf("clean database: %v", err)
	}

	var datasetID, runID string
	err = database.QueryRowContext(ctx, `
		INSERT INTO datasets (source_type, source_sha256, source_schema_version, filename)
		VALUES ('nyc-yellow', $1, 'v1', 'rollback-fixture.parquet') RETURNING id`, "cd"+repeat("0", 62)).Scan(&datasetID)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `
		INSERT INTO replay_runs (dataset_id, state, started_at)
		VALUES ($1, 'RUNNING', now()) RETURNING id`, datasetID).Scan(&runID)
	if err != nil {
		t.Fatal(err)
	}

	pickupAt := time.Date(2024, 1, 2, 8, 15, 0, 0, time.UTC)
	pickupHour := pickupAt.Truncate(time.Hour)
	const maxInt64 = int64(9223372036854775807)
	_, err = database.ExecContext(ctx, `
		INSERT INTO hourly_zone_stats (
			dataset_id, pickup_zone_id, pickup_hour_utc, trip_count,
			total_distance_milli_miles, total_fare_cents, fare_observed_count
		) VALUES ($1, 10, $2, $3, 0, 0, 0)`, datasetID, pickupHour, maxInt64)
	if err != nil {
		t.Fatal(err)
	}

	event := domain.TripEvent{
		DatasetID: datasetID, RunID: runID, EventID: repeat("2", 64), SourceRowNumber: 0,
		PickupAt:     pickupAt,
		DropoffAt:    pickupAt.Add(15 * time.Minute),
		PickupZoneID: 10, DistanceMilliMiles: 2500,
	}
	repository := NewEventRepository(database)

	if result, processErr := repository.ProcessEvent(ctx, event); processErr == nil {
		t.Fatalf("expected aggregate overflow to fail, got result %+v", result)
	}

	var tripRows, outcomeRows int64
	err = database.QueryRowContext(ctx, `SELECT count(*) FROM trip_events WHERE dataset_id = $1 AND event_id = $2`, datasetID, event.EventID).Scan(&tripRows)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `SELECT count(*) FROM run_event_outcomes WHERE run_id = $1 AND event_id = $2`, runID, event.EventID).Scan(&outcomeRows)
	if err != nil {
		t.Fatal(err)
	}

	var trips, distance, fares, fareCount int64
	err = database.QueryRowContext(ctx, `
		SELECT trip_count, total_distance_milli_miles, total_fare_cents, fare_observed_count
		FROM hourly_zone_stats
		WHERE dataset_id = $1 AND pickup_zone_id = 10 AND pickup_hour_utc = $2`, datasetID, pickupHour).
		Scan(&trips, &distance, &fares, &fareCount)
	if err != nil {
		t.Fatal(err)
	}
	if tripRows != 0 || outcomeRows != 0 {
		t.Fatalf("failed transaction left partial rows: trips=%d outcomes=%d", tripRows, outcomeRows)
	}
	if trips != maxInt64 || distance != 0 || fares != 0 || fareCount != 0 {
		t.Fatalf("failed transaction changed aggregate: %d %d %d %d", trips, distance, fares, fareCount)
	}

	_, err = database.ExecContext(ctx, `
		DELETE FROM hourly_zone_stats
		WHERE dataset_id = $1 AND pickup_zone_id = 10 AND pickup_hour_utc = $2`, datasetID, pickupHour)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.ProcessEvent(ctx, event)
	if err != nil || result.Outcome != domain.OutcomeAccepted {
		t.Fatalf("retry after rollback = %+v, %v", result, err)
	}
	err = database.QueryRowContext(ctx, `
		SELECT trip_count, total_distance_milli_miles, total_fare_cents, fare_observed_count
		FROM hourly_zone_stats
		WHERE dataset_id = $1 AND pickup_zone_id = 10 AND pickup_hour_utc = $2`, datasetID, pickupHour).
		Scan(&trips, &distance, &fares, &fareCount)
	if err != nil {
		t.Fatal(err)
	}
	if trips != 1 || distance != 2500 || fares != 0 || fareCount != 0 {
		t.Fatalf("retry after rollback produced wrong aggregate: %d %d %d %d", trips, distance, fares, fareCount)
	}
}

func repeat(value string, count int) string {
	result := ""
	for range count {
		result += value
	}
	return result
}
