package restapi_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
	postgresrepo "github.com/example/streamforge/services/core-go/internal/postgres"
	"github.com/example/streamforge/services/core-go/internal/restapi"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestRESTWorkflowUsesDurableDatabaseState(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	ctx := context.Background()
	if err = database.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = database.ExecContext(ctx, `SELECT pg_advisory_lock(8291042024)`); err != nil {
		t.Fatal(err)
	}
	if _, err = database.ExecContext(ctx, `TRUNCATE run_event_outcomes,hourly_zone_stats,rejected_events,trip_events,replay_runs,datasets CASCADE`); err != nil {
		t.Fatal(err)
	}

	httpServer := httptest.NewServer(restapi.NewServer(postgresrepo.NewCatalogRepository(database)).Handler())
	defer httpServer.Close()
	sourceSHA := "ab" + repeat("0", 62)
	datasetBody := map[string]any{"source_type": "nyc-yellow", "source_sha256": sourceSHA, "source_schema_version": "v1", "filename": "api-fixture.parquet", "source_size_bytes": 4096}
	status, dataset := requestJSON(t, httpServer.Client(), "POST", httpServer.URL+"/api/v1/datasets", datasetBody)
	if status != http.StatusCreated {
		t.Fatalf("dataset status=%d body=%v", status, dataset)
	}
	datasetID := dataset["id"].(string)
	status, _ = requestJSON(t, httpServer.Client(), "POST", httpServer.URL+"/api/v1/datasets", datasetBody)
	if status != http.StatusOK {
		t.Fatalf("idempotent dataset status=%d", status)
	}

	status, run := requestJSON(t, httpServer.Client(), "POST", httpServer.URL+"/api/v1/runs", map[string]any{"dataset_id": datasetID})
	if status != http.StatusCreated {
		t.Fatalf("run status=%d body=%v", status, run)
	}
	runID := run["id"].(string)
	status, runPage := requestJSON(t, httpServer.Client(), "GET", httpServer.URL+"/api/v1/runs?dataset_id="+datasetID+"&state=RUNNING&limit=10", nil)
	if status != http.StatusOK || len(runPage["items"].([]any)) != 1 {
		t.Fatalf("run listing status=%d body=%v", status, runPage)
	}
	events := postgresrepo.NewEventRepository(database)
	pickup := time.Date(2024, 1, 4, 10, 15, 0, 0, time.UTC)
	fare := int64(1500)
	_, err = events.ProcessEvent(ctx, domain.TripEvent{DatasetID: datasetID, RunID: runID, EventID: repeat("1", 64), SourceSHA256: sourceSHA, PickupAt: pickup, DropoffAt: pickup.Add(10 * time.Minute), PickupZoneID: 10, DistanceMilliMiles: 2000, FareCents: &fare})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		_, err = events.ReportSourceRejection(ctx, domain.SourceRejection{DatasetID: datasetID, RunID: runID, EventID: repeat(fmt.Sprint(index+2), 64), SourceRowNumber: uint64(index + 1), ReasonCode: "INVALID_PICKUP_ZONE", Detail: "invalid fixture zone", ValidationPolicyVersion: "v1"})
		if err != nil {
			t.Fatal(err)
		}
	}

	analyticsURL := fmt.Sprintf("%s/api/v1/analytics/zone-hourly?dataset_id=%s&start=2024-01-04T10:00:00Z&end=2024-01-04T11:00:00Z&pickup_zone_id=10", httpServer.URL, datasetID)
	status, analytics := requestJSON(t, httpServer.Client(), "GET", analyticsURL, nil)
	if status != 200 || len(analytics["items"].([]any)) != 1 {
		t.Fatalf("analytics status=%d body=%v", status, analytics)
	}
	status, firstPage := requestJSON(t, httpServer.Client(), "GET", fmt.Sprintf("%s/api/v1/quality/rejections?dataset_id=%s&limit=1", httpServer.URL, datasetID), nil)
	if status != 200 || len(firstPage["items"].([]any)) != 1 || firstPage["next_cursor"] == nil {
		t.Fatalf("rejections page=%v", firstPage)
	}
	status, secondPage := requestJSON(t, httpServer.Client(), "GET", fmt.Sprintf("%s/api/v1/quality/rejections?dataset_id=%s&limit=1&cursor=%s", httpServer.URL, datasetID, firstPage["next_cursor"]), nil)
	if status != 200 || len(secondPage["items"].([]any)) != 1 {
		t.Fatalf("second page=%v", secondPage)
	}

	status, currentRun := requestJSON(t, httpServer.Client(), "GET", httpServer.URL+"/api/v1/runs/"+runID, nil)
	if status != 200 || currentRun["input_count"].(float64) != 3 || currentRun["accepted_count"].(float64) != 1 || currentRun["rejected_count"].(float64) != 2 {
		t.Fatalf("run counters=%v", currentRun)
	}
	status, conflict := requestJSON(t, httpServer.Client(), "POST", httpServer.URL+"/api/v1/runs/"+runID+"/complete", map[string]any{"expected_input_count": 4})
	if status != http.StatusConflict || conflict["request_id"] == "" {
		t.Fatalf("completion conflict status=%d body=%v", status, conflict)
	}
	status, completed := requestJSON(t, httpServer.Client(), "POST", httpServer.URL+"/api/v1/runs/"+runID+"/complete", map[string]any{"expected_input_count": 3})
	if status != 200 || completed["state"] != "COMPLETED" {
		t.Fatalf("completed=%v", completed)
	}
	status, dataset = requestJSON(t, httpServer.Client(), "GET", httpServer.URL+"/api/v1/datasets/"+datasetID, nil)
	if status != 200 || dataset["has_completed_run"] != true {
		t.Fatalf("dataset completeness=%v", dataset)
	}
}

func requestJSON(t *testing.T, client *http.Client, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	if err = json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, decoded
}
func repeat(value string, count int) string {
	result := ""
	for range count {
		result += value
	}
	return result
}
