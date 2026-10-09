package restapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/example/streamforge/services/core-go/internal/application"
	"github.com/example/streamforge/services/core-go/internal/domain"
)

const maxBodyBytes = 1 << 20

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type Server struct {
	repository application.CatalogRepository
	lagReader  application.ConsumerLagReader
	mux        *http.ServeMux
}

type Option func(*Server)

func WithConsumerLag(reader application.ConsumerLagReader) Option {
	return func(server *Server) { server.lagReader = reader }
}

func NewServer(repository application.CatalogRepository, options ...Option) *Server {
	server := &Server{repository: repository, mux: http.NewServeMux()}
	for _, option := range options {
		option(server)
	}
	server.mux.HandleFunc("GET /health/live", server.live)
	server.mux.HandleFunc("GET /health/ready", server.ready)
	server.mux.HandleFunc("GET /metrics", server.metrics)
	server.mux.HandleFunc("POST /api/v1/datasets", server.registerDataset)
	server.mux.HandleFunc("GET /api/v1/datasets", server.listDatasets)
	server.mux.HandleFunc("GET /api/v1/datasets/{id}", server.getDataset)
	server.mux.HandleFunc("POST /api/v1/runs", server.createRun)
	server.mux.HandleFunc("GET /api/v1/runs", server.listRuns)
	server.mux.HandleFunc("GET /api/v1/runs/{id}", server.getRun)
	server.mux.HandleFunc("POST /api/v1/runs/{id}/complete", server.completeRun)
	server.mux.HandleFunc("POST /api/v1/runs/{id}/cancel", server.cancelRun)
	server.mux.HandleFunc("GET /api/v1/analytics/zone-hourly", server.zoneHourly)
	server.mux.HandleFunc("GET /api/v1/quality/rejections", server.rejections)
	server.mux.HandleFunc("GET /api/v1/operations/kafka-lag", server.kafkaLag)
	return server
}

func (server *Server) kafkaLag(w http.ResponseWriter, r *http.Request) {
	if server.lagReader == nil {
		writeJSON(w, http.StatusOK, map[string]any{"enabled": false, "available": false})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	lag, err := server.lagReader.ReadConsumerLag(ctx)
	if err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "Kafka consumer lag is unavailable", nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"enabled": true, "available": true, "consumer": lag})
}

func (server *Server) Handler() http.Handler { return requestIDMiddleware(server.mux) }

func (server *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
}
func (server *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := server.repository.Ping(r.Context()); err != nil {
		writeError(w, r, http.StatusServiceUnavailable, "DEPENDENCY_UNAVAILABLE", "database is unavailable", nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (server *Server) registerDataset(w http.ResponseWriter, r *http.Request) {
	var input application.DatasetRegistration
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, r, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error(), nil)
		return
	}
	if input.SourceType != "nyc-yellow" || input.SourceSchemaVersion != "v1" || input.Filename == "" || input.SourceSizeBytes < 0 || !lowerHexSHA(input.SourceSHA256) {
		writeError(w, r, http.StatusBadRequest, "UNSUPPORTED_DATASET", "expected nyc-yellow v1 metadata with lowercase SHA-256, filename, and non-negative size", nil)
		return
	}
	item, created, err := server.repository.RegisterDataset(r.Context(), input)
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, item)
}

func (server *Server) listDatasets(w http.ResponseWriter, r *http.Request) {
	limit, ok := queryLimit(w, r, 50, 100)
	if !ok {
		return
	}
	page, err := server.repository.ListDatasets(r.Context(), limit, r.URL.Query().Get("cursor"))
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": page.Items, "next_cursor": emptyAsNil(page.NextCursor)})
}
func (server *Server) getDataset(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, r, 400, "INVALID_ARGUMENT", "dataset id must be a UUID", nil)
		return
	}
	item, err := server.repository.GetDataset(r.Context(), id)
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, 200, item)
}

func (server *Server) createRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DatasetID string `json:"dataset_id"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		writeError(w, r, 400, "INVALID_ARGUMENT", err.Error(), nil)
		return
	}
	if !uuidPattern.MatchString(body.DatasetID) {
		writeError(w, r, 400, "INVALID_ARGUMENT", "dataset_id must be a UUID", nil)
		return
	}
	item, err := server.repository.CreateRun(r.Context(), body.DatasetID)
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, item)
}
func (server *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	datasetID := q.Get("dataset_id")
	if datasetID != "" && !uuidPattern.MatchString(datasetID) {
		writeError(w, r, 400, "INVALID_ARGUMENT", "dataset_id must be a UUID", nil)
		return
	}
	state := domain.RunState(q.Get("state"))
	if state != "" && state != domain.RunCreated && state != domain.RunRunning && state != domain.RunCompleted && state != domain.RunFailed && state != domain.RunCancelled {
		writeError(w, r, 400, "INVALID_ARGUMENT", "state is invalid", nil)
		return
	}
	limit, ok := queryLimit(w, r, 50, 100)
	if !ok {
		return
	}
	page, err := server.repository.ListRuns(r.Context(), datasetID, state, limit, q.Get("cursor"))
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": page.Items, "next_cursor": emptyAsNil(page.NextCursor)})
}
func (server *Server) getRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, r, 400, "INVALID_ARGUMENT", "run id must be a UUID", nil)
		return
	}
	item, err := server.repository.GetRun(r.Context(), id)
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, 200, item)
}
func (server *Server) completeRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		ExpectedInputCount int64 `json:"expected_input_count"`
	}
	if !uuidPattern.MatchString(id) {
		writeError(w, r, 400, "INVALID_ARGUMENT", "run id must be a UUID", nil)
		return
	}
	if err := decodeJSON(w, r, &body); err != nil || body.ExpectedInputCount < 0 {
		writeError(w, r, 400, "INVALID_ARGUMENT", "expected_input_count must be a non-negative integer", nil)
		return
	}
	item, err := server.repository.CompleteRun(r.Context(), id, body.ExpectedInputCount)
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, 200, item)
}
func (server *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, r, 400, "INVALID_ARGUMENT", "run id must be a UUID", nil)
		return
	}
	item, err := server.repository.CancelRun(r.Context(), id)
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, 200, item)
}

func (server *Server) zoneHourly(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	datasetID := q.Get("dataset_id")
	if !uuidPattern.MatchString(datasetID) {
		writeError(w, r, 400, "INVALID_ARGUMENT", "dataset_id must be a UUID", nil)
		return
	}
	start, err1 := time.Parse(time.RFC3339, q.Get("start"))
	end, err2 := time.Parse(time.RFC3339, q.Get("end"))
	if err1 != nil || err2 != nil || !start.Before(end) || end.Sub(start) > 366*24*time.Hour {
		writeError(w, r, 400, "INVALID_ARGUMENT", "start and end must be RFC3339, ordered, and at most 366 days apart", nil)
		return
	}
	var zone *int32
	if raw := q.Get("pickup_zone_id"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || value < 1 {
			writeError(w, r, 400, "INVALID_ARGUMENT", "pickup_zone_id must be positive", nil)
			return
		}
		converted := int32(value)
		zone = &converted
	}
	limit, ok := queryLimit(w, r, 500, 1000)
	if !ok {
		return
	}
	items, complete, err := server.repository.ListHourlyZoneStats(r.Context(), datasetID, start, end, zone, limit)
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "dataset_complete": complete})
}
func (server *Server) rejections(w http.ResponseWriter, r *http.Request) {
	datasetID := r.URL.Query().Get("dataset_id")
	if !uuidPattern.MatchString(datasetID) {
		writeError(w, r, 400, "INVALID_ARGUMENT", "dataset_id must be a UUID", nil)
		return
	}
	limit, ok := queryLimit(w, r, 50, 200)
	if !ok {
		return
	}
	page, err := server.repository.ListRejections(r.Context(), datasetID, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		writeRepositoryError(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": page.Items, "next_cursor": emptyAsNil(page.NextCursor)})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}
func queryLimit(w http.ResponseWriter, r *http.Request, defaultValue, max int) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultValue, true
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > max {
		writeError(w, r, 400, "INVALID_ARGUMENT", fmt.Sprintf("limit must be between 1 and %d", max), nil)
		return 0, false
	}
	return value, true
}
func lowerHexSHA(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		totalRequests.Add(1)
		id := r.Header.Get("X-Request-ID")
		if !requestIDPattern.MatchString(id) {
			bytes := make([]byte, 16)
			_, _ = rand.Read(bytes)
			id = hex.EncodeToString(bytes)
		}
		w.Header().Set("X-Request-ID", id)
		r.Header.Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}
func writeRepositoryError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, application.ErrNotFound):
		writeError(w, r, 404, "NOT_FOUND", "resource was not found", nil)
	case errors.Is(err, application.ErrConflict):
		writeError(w, r, 409, "CONFLICT", err.Error(), nil)
	case strings.Contains(err.Error(), "invalid cursor"):
		writeError(w, r, 400, "INVALID_CURSOR", "cursor is invalid", nil)
	default:
		writeError(w, r, 500, "INTERNAL", "internal server error", nil)
	}
}
func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, details any) {
	writeJSON(w, status, map[string]any{"code": code, "message": message, "request_id": r.Header.Get("X-Request-ID"), "details": details})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func emptyAsNil(value string) any {
	if value == "" {
		return nil
	}
	return value
}
