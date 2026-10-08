package application

import (
	"context"
	"errors"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

type DatasetRegistration struct {
	SourceType          string `json:"source_type"`
	SourceSHA256        string `json:"source_sha256"`
	SourceSchemaVersion string `json:"source_schema_version"`
	Filename            string `json:"filename"`
	SourceSizeBytes     int64  `json:"source_size_bytes"`
}

type DatasetPage struct {
	Items      []domain.Dataset
	NextCursor string
}

type RejectionPage struct {
	Items      []domain.RejectionRecord
	NextCursor string
}

type RunPage struct {
	Items      []domain.ReplayRun
	NextCursor string
}

type CatalogRepository interface {
	Ping(ctx context.Context) error
	RegisterDataset(ctx context.Context, registration DatasetRegistration) (domain.Dataset, bool, error)
	ListDatasets(ctx context.Context, limit int, cursor string) (DatasetPage, error)
	GetDataset(ctx context.Context, datasetID string) (domain.Dataset, error)
	CreateRun(ctx context.Context, datasetID string) (domain.ReplayRun, error)
	ListRuns(ctx context.Context, datasetID string, state domain.RunState, limit int, cursor string) (RunPage, error)
	GetRun(ctx context.Context, runID string) (domain.ReplayRun, error)
	CompleteRun(ctx context.Context, runID string, expectedInputCount int64) (domain.ReplayRun, error)
	CancelRun(ctx context.Context, runID string) (domain.ReplayRun, error)
	ListHourlyZoneStats(ctx context.Context, datasetID string, start, end time.Time, pickupZoneID *int32, limit int) ([]domain.HourlyZoneStat, bool, error)
	ListRejections(ctx context.Context, datasetID string, limit int, cursor string) (RejectionPage, error)
}
