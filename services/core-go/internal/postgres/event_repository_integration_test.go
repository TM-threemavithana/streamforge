package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestProcessEventIsIdempotentAcrossRetriesAndRuns(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	lockIntegrationDatabase(t, ctx, database)

	_, err = database.ExecContext(ctx, `TRUNCATE run_event_outcomes, hourly_zone_stats, rejected_events, trip_events, replay_runs, datasets CASCADE`)
	if err != nil {
		t.Fatalf("clean database: %v", err)
	}

	var datasetID, firstRunID, secondRunID string
	err = database.QueryRowContext(ctx, `
		INSERT INTO datasets (source_type, source_sha256, source_schema_version, filename, source_size_bytes)
		VALUES ('nyc-yellow', $1, 'v1', 'fixture.parquet', 0) RETURNING id`, "ab"+repeat("0", 62)).Scan(&datasetID)
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
		DatasetID: datasetID, RunID: firstRunID, EventID: repeat("1", 64), SourceSHA256: "ab" + repeat("0", 62), SourceRowNumber: 0,
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
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	lockIntegrationDatabase(t, ctx, database)

	_, err = database.ExecContext(ctx, `TRUNCATE run_event_outcomes, hourly_zone_stats, rejected_events, trip_events, replay_runs, datasets CASCADE`)
	if err != nil {
		t.Fatalf("clean database: %v", err)
	}

	var datasetID, runID string
	err = database.QueryRowContext(ctx, `
		INSERT INTO datasets (source_type, source_sha256, source_schema_version, filename, source_size_bytes)
		VALUES ('nyc-yellow', $1, 'v1', 'rollback-fixture.parquet', 0) RETURNING id`, "cd"+repeat("0", 62)).Scan(&datasetID)
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
		DatasetID: datasetID, RunID: runID, EventID: repeat("2", 64), SourceSHA256: "cd" + repeat("0", 62), SourceRowNumber: 0,
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

func TestReportSourceRejectionIsDurableAndDoesNotContributeToAggregates(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	lockIntegrationDatabase(t, ctx, database)

	_, err = database.ExecContext(ctx, `TRUNCATE run_event_outcomes, hourly_zone_stats, rejected_events, trip_events, replay_runs, datasets CASCADE`)
	if err != nil {
		t.Fatalf("clean database: %v", err)
	}

	var datasetID, runID string
	err = database.QueryRowContext(ctx, `
		INSERT INTO datasets (source_type, source_sha256, source_schema_version, filename, source_size_bytes)
		VALUES ('nyc-yellow', $1, 'v1', 'rejection-fixture.parquet', 0) RETURNING id`, "ef"+repeat("0", 62)).Scan(&datasetID)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `
		INSERT INTO replay_runs (dataset_id, state, started_at)
		VALUES ($1, 'RUNNING', now()) RETURNING id`, datasetID).Scan(&runID)
	if err != nil {
		t.Fatal(err)
	}

	rejection := domain.SourceRejection{
		DatasetID:               datasetID,
		RunID:                   runID,
		EventID:                 repeat("3", 64),
		SourceRowNumber:         7,
		ReasonCode:              "INVALID_PICKUP_ZONE",
		Detail:                  "zone 999 is not in the approved lookup",
		ValidationPolicyVersion: "nyc-yellow-validation:v1",
	}
	repository := NewEventRepository(database)

	first, err := repository.ReportSourceRejection(ctx, rejection)
	if err != nil || first.Outcome != domain.OutcomeRejected || first.ReasonCode != rejection.ReasonCode {
		t.Fatalf("first rejection = %+v, %v", first, err)
	}
	retry, err := repository.ReportSourceRejection(ctx, rejection)
	if err != nil || retry != first {
		t.Fatalf("rejection retry = %+v, %v; want %+v", retry, err, first)
	}

	var rejectedRows, tripRows, aggregateRows, outcomeRows int64
	err = database.QueryRowContext(ctx, `SELECT count(*) FROM rejected_events WHERE dataset_id = $1 AND event_id = $2`, datasetID, rejection.EventID).Scan(&rejectedRows)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `SELECT count(*) FROM trip_events WHERE dataset_id = $1 AND event_id = $2`, datasetID, rejection.EventID).Scan(&tripRows)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `SELECT count(*) FROM hourly_zone_stats WHERE dataset_id = $1`, datasetID).Scan(&aggregateRows)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `SELECT count(*) FROM run_event_outcomes WHERE run_id = $1 AND event_id = $2 AND outcome = 'REJECTED'`, runID, rejection.EventID).Scan(&outcomeRows)
	if err != nil {
		t.Fatal(err)
	}
	if rejectedRows != 1 || tripRows != 0 || aggregateRows != 0 || outcomeRows != 1 {
		t.Fatalf("rejection persistence mismatch: rejected=%d trips=%d aggregates=%d outcomes=%d", rejectedRows, tripRows, aggregateRows, outcomeRows)
	}
}

func TestAcceptedAndRejectedRequestsCannotCommitConflictingSideEffects(t *testing.T) {
	database := openConcurrentIntegrationDatabase(t)
	ctx := context.Background()
	_, err := database.ExecContext(ctx, `TRUNCATE run_event_outcomes, hourly_zone_stats, rejected_events, trip_events, replay_runs, datasets CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	var datasetID, runID string
	sourceSHA := "aa" + repeat("0", 62)
	if err = database.QueryRowContext(ctx, `INSERT INTO datasets (source_type,source_sha256,source_schema_version,filename,source_size_bytes) VALUES ('nyc-yellow',$1,'v1','race.parquet',1) RETURNING id`, sourceSHA).Scan(&datasetID); err != nil {
		t.Fatal(err)
	}
	if err = database.QueryRowContext(ctx, `INSERT INTO replay_runs (dataset_id,state,started_at) VALUES ($1,'RUNNING',now()) RETURNING id`, datasetID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	repository := NewEventRepository(database)
	acceptedWinners := int64(0)
	for index := 0; index < 20; index++ {
		eventID := fmt.Sprintf("%064x", index+1)
		pickup := time.Date(2024, 1, 5, 10, 0, 0, 0, time.UTC).Add(time.Duration(index) * time.Hour)
		event := domain.TripEvent{DatasetID: datasetID, RunID: runID, EventID: eventID, SourceSHA256: sourceSHA, SourceRowNumber: uint64(index), PickupAt: pickup, DropoffAt: pickup.Add(time.Minute), PickupZoneID: 10, DistanceMilliMiles: 1000}
		rejection := domain.SourceRejection{DatasetID: datasetID, RunID: runID, EventID: eventID, SourceRowNumber: uint64(index), ReasonCode: "TEST_REJECTION", Detail: "competing request", ValidationPolicyVersion: "v1"}
		start := make(chan struct{})
		results := make(chan domain.EventResult, 2)
		errorsChannel := make(chan error, 2)
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			value, callErr := repository.ProcessEvent(ctx, event)
			results <- value
			errorsChannel <- callErr
		}()
		go func() {
			defer wait.Done()
			<-start
			value, callErr := repository.ReportSourceRejection(ctx, rejection)
			results <- value
			errorsChannel <- callErr
		}()
		close(start)
		wait.Wait()
		close(results)
		close(errorsChannel)
		for callErr := range errorsChannel {
			if callErr != nil {
				t.Fatalf("competing request failed: %v", callErr)
			}
		}
		var winner domain.Outcome
		for value := range results {
			if winner == "" {
				winner = value.Outcome
			}
			if value.Outcome != winner {
				t.Fatalf("unstable outcomes for %s: got %s and %s", eventID, winner, value.Outcome)
			}
		}
		var trips, rejections int64
		if err = database.QueryRowContext(ctx, `SELECT count(*) FROM trip_events WHERE dataset_id=$1 AND event_id=$2`, datasetID, eventID).Scan(&trips); err != nil {
			t.Fatal(err)
		}
		if err = database.QueryRowContext(ctx, `SELECT count(*) FROM rejected_events WHERE dataset_id=$1 AND event_id=$2`, datasetID, eventID).Scan(&rejections); err != nil {
			t.Fatal(err)
		}
		if winner == domain.OutcomeAccepted {
			acceptedWinners++
			if trips != 1 || rejections != 0 {
				t.Fatalf("accepted outcome has trips=%d rejections=%d", trips, rejections)
			}
		} else if winner == domain.OutcomeRejected {
			if trips != 0 || rejections != 1 {
				t.Fatalf("rejected outcome has trips=%d rejections=%d", trips, rejections)
			}
		} else {
			t.Fatalf("unexpected winner %s", winner)
		}
	}
	var aggregateTrips int64
	if err = database.QueryRowContext(ctx, `SELECT COALESCE(sum(trip_count),0) FROM hourly_zone_stats WHERE dataset_id=$1`, datasetID).Scan(&aggregateTrips); err != nil {
		t.Fatal(err)
	}
	if aggregateTrips != acceptedWinners {
		t.Fatalf("aggregate trips=%d accepted winners=%d", aggregateTrips, acceptedWinners)
	}
}

func TestCompletionWaitsForInFlightEventTransaction(t *testing.T) {
	database := openConcurrentIntegrationDatabase(t)
	ctx := context.Background()
	_, err := database.ExecContext(ctx, `TRUNCATE run_event_outcomes, hourly_zone_stats, rejected_events, trip_events, replay_runs, datasets CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION streamforge_test_delay_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.25); RETURN NEW; END $$;
		CREATE TRIGGER streamforge_test_delay_outcome_trigger BEFORE INSERT ON run_event_outcomes FOR EACH ROW EXECUTE FUNCTION streamforge_test_delay_outcome()`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = database.ExecContext(context.Background(), `DROP TRIGGER IF EXISTS streamforge_test_delay_outcome_trigger ON run_event_outcomes; DROP FUNCTION IF EXISTS streamforge_test_delay_outcome()`)
	})
	var datasetID, runID string
	sourceSHA := "bb" + repeat("0", 62)
	if err = database.QueryRowContext(ctx, `INSERT INTO datasets (source_type,source_sha256,source_schema_version,filename,source_size_bytes) VALUES ('nyc-yellow',$1,'v1','terminal-race.parquet',1) RETURNING id`, sourceSHA).Scan(&datasetID); err != nil {
		t.Fatal(err)
	}
	if err = database.QueryRowContext(ctx, `INSERT INTO replay_runs (dataset_id,state,started_at) VALUES ($1,'RUNNING',now()) RETURNING id`, datasetID).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	pickup := time.Date(2024, 1, 6, 12, 0, 0, 0, time.UTC)
	event := domain.TripEvent{DatasetID: datasetID, RunID: runID, EventID: repeat("c", 64), SourceSHA256: sourceSHA, PickupAt: pickup, DropoffAt: pickup.Add(time.Minute), PickupZoneID: 10, DistanceMilliMiles: 1000}
	eventDone := make(chan error, 1)
	go func() { _, callErr := NewEventRepository(database).ProcessEvent(ctx, event); eventDone <- callErr }()
	time.Sleep(75 * time.Millisecond)
	run, completeErr := NewCatalogRepository(database).CompleteRun(ctx, runID, 1)
	if completeErr != nil {
		t.Fatalf("complete while event in flight: %v", completeErr)
	}
	if eventErr := <-eventDone; eventErr != nil {
		t.Fatalf("in-flight event: %v", eventErr)
	}
	if run.State != domain.RunCompleted || run.AcceptedCount != 1 || run.InputCount != 1 {
		t.Fatalf("completed run has stale counters: %+v", run)
	}
}

func repeat(value string, count int) string {
	result := ""
	for range count {
		result += value
	}
	return result
}

func lockIntegrationDatabase(t *testing.T, ctx context.Context, database *sql.DB) {
	t.Helper()
	// One pooled session holds this lock until database.Close, serializing tests
	// that truncate the shared integration database across Go packages.
	database.SetMaxOpenConns(1)
	if _, err := database.ExecContext(ctx, `SELECT pg_advisory_lock(8291042024)`); err != nil {
		t.Fatalf("lock integration database: %v", err)
	}
}

func openConcurrentIntegrationDatabase(t *testing.T) *sql.DB {
	t.Helper()
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(8)
	if err = database.Ping(); err != nil {
		database.Close()
		t.Fatal(err)
	}
	lockConnection, err := database.Conn(context.Background())
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if _, err = lockConnection.ExecContext(context.Background(), `SELECT pg_advisory_lock(8291042024)`); err != nil {
		lockConnection.Close()
		database.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = lockConnection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(8291042024)`)
		_ = lockConnection.Close()
		_ = database.Close()
	})
	return database
}
