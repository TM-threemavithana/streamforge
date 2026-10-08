package ingestgrpc

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	ingestv1 "github.com/example/streamforge/services/core-go/gen/streamforge/ingest/v1"
	postgresrepo "github.com/example/streamforge/services/core-go/internal/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPythonClientRetriesResponseLossWithoutDoubleCounting(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	python, err := exec.LookPath("python")
	if err != nil {
		t.Skip("python executable is required for the cross-language integration test")
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
	database.SetMaxOpenConns(1)
	if _, err = database.ExecContext(ctx, `SELECT pg_advisory_lock(8291042024)`); err != nil {
		t.Fatal(err)
	}
	if _, err = database.ExecContext(ctx, `TRUNCATE run_event_outcomes, hourly_zone_stats, rejected_events, trip_events, replay_runs, datasets CASCADE`); err != nil {
		t.Fatal(err)
	}

	sourceSHA := "ab" + repeatText("0", 62)
	var datasetID, runID string
	err = database.QueryRowContext(ctx, `
		INSERT INTO datasets (source_type, source_sha256, source_schema_version, filename, source_size_bytes)
		VALUES ('nyc-yellow', $1, 'v1', 'grpc-fixture.parquet', 0) RETURNING id`, sourceSHA).Scan(&datasetID)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `
		INSERT INTO replay_runs (dataset_id, state, started_at)
		VALUES ($1, 'RUNNING', now()) RETURNING id`, datasetID).Scan(&runID)
	if err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var ingestCalls atomic.Int32
	responseLoss := func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		response, handlerErr := handler(ctx, request)
		if handlerErr == nil && info.FullMethod == ingestv1.TripIngestionService_IngestBatch_FullMethodName && ingestCalls.Add(1) == 1 {
			return nil, status.Error(codes.Unavailable, "simulated response loss after database commit")
		}
		return response, handlerErr
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(responseLoss))
	ingestv1.RegisterTripIngestionServiceServer(grpcServer, NewServer(postgresrepo.NewEventRepository(database)))
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repositoryRoot := filepath.Clean(filepath.Join(workingDirectory, "..", "..", "..", ".."))
	probe := filepath.Join(repositoryRoot, "tests", "integration", "python_grpc_probe.py")
	pythonSource := filepath.Join(repositoryRoot, "tools", "replay-python", "src")
	eventID := repeatText("4", 64)
	command := exec.Command(python, probe,
		"--target", listener.Addr().String(),
		"--dataset-id", datasetID,
		"--run-id", runID,
		"--event-id", eventID,
		"--source-sha256", sourceSHA,
	)
	command.Env = append(os.Environ(), "PYTHONPATH="+pythonSource)
	var standardOutput, standardError bytes.Buffer
	command.Stdout = &standardOutput
	command.Stderr = &standardError
	err = command.Run()
	if err != nil {
		t.Fatalf("python gRPC probe: %v\n%s", err, standardError.String())
	}
	var results []struct {
		EventID  string `json:"event_id"`
		Outcome  string `json:"outcome"`
		AckStage string `json:"ack_stage"`
	}
	if err = json.Unmarshal(standardOutput.Bytes(), &results); err != nil {
		t.Fatalf("decode Python response %q (stderr %q): %v", standardOutput.String(), standardError.String(), err)
	}
	if len(results) != 1 || results[0].EventID != eventID || results[0].Outcome != "OUTCOME_ACCEPTED" || results[0].AckStage != "ACK_STAGE_DATABASE_COMMITTED" {
		t.Fatalf("unexpected Python response: %+v", results)
	}
	if ingestCalls.Load() != 2 {
		t.Fatalf("ingest calls = %d; want 2", ingestCalls.Load())
	}

	var eventRows, outcomeRows, tripCount, distance, fares, fareCount int64
	err = database.QueryRowContext(ctx, `SELECT count(*) FROM trip_events WHERE dataset_id = $1 AND event_id = $2`, datasetID, eventID).Scan(&eventRows)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `SELECT count(*) FROM run_event_outcomes WHERE run_id = $1 AND event_id = $2`, runID, eventID).Scan(&outcomeRows)
	if err != nil {
		t.Fatal(err)
	}
	err = database.QueryRowContext(ctx, `
		SELECT trip_count, total_distance_milli_miles, total_fare_cents, fare_observed_count
		FROM hourly_zone_stats WHERE dataset_id = $1`, datasetID).
		Scan(&tripCount, &distance, &fares, &fareCount)
	if err != nil {
		t.Fatal(err)
	}
	if eventRows != 1 || outcomeRows != 1 || tripCount != 1 || distance != 3200 || fares != 1450 || fareCount != 1 {
		t.Fatal(fmt.Sprintf("double-count protection failed: events=%d outcomes=%d aggregate=%d/%d/%d/%d", eventRows, outcomeRows, tripCount, distance, fares, fareCount))
	}
}

func repeatText(value string, count int) string {
	result := ""
	for range count {
		result += value
	}
	return result
}
