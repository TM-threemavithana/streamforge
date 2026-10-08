package postgres

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/example/streamforge/services/core-go/internal/application"
	"github.com/example/streamforge/services/core-go/internal/domain"
)

type CatalogRepository struct{ database *sql.DB }

func NewCatalogRepository(database *sql.DB) *CatalogRepository {
	return &CatalogRepository{database: database}
}

func (repository *CatalogRepository) Ping(ctx context.Context) error {
	return repository.database.PingContext(ctx)
}

func (repository *CatalogRepository) RegisterDataset(ctx context.Context, input application.DatasetRegistration) (domain.Dataset, bool, error) {
	var id string
	err := repository.database.QueryRowContext(ctx, `
		INSERT INTO datasets (source_type, source_sha256, source_schema_version, filename, source_size_bytes)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (source_type, source_sha256) DO NOTHING
		RETURNING id`, input.SourceType, input.SourceSHA256, input.SourceSchemaVersion, input.Filename, input.SourceSizeBytes).Scan(&id)
	created := true
	if errors.Is(err, sql.ErrNoRows) {
		created = false
		err = repository.database.QueryRowContext(ctx, `SELECT id FROM datasets WHERE source_type=$1 AND source_sha256=$2`, input.SourceType, input.SourceSHA256).Scan(&id)
	}
	if err != nil {
		return domain.Dataset{}, false, fmt.Errorf("register dataset: %w", err)
	}
	dataset, err := repository.GetDataset(ctx, id)
	return dataset, created, err
}

const datasetColumns = `d.id,d.source_type,d.source_sha256,d.source_schema_version,d.filename,d.source_size_bytes,d.status,d.created_at,
	EXISTS (SELECT 1 FROM replay_runs completed WHERE completed.dataset_id=d.id AND completed.state='COMPLETED'), latest.state`

func scanDataset(scanner interface{ Scan(...any) error }) (domain.Dataset, error) {
	var item domain.Dataset
	var latest sql.NullString
	err := scanner.Scan(&item.ID, &item.SourceType, &item.SourceSHA256, &item.SourceSchemaVersion, &item.Filename, &item.SourceSizeBytes, &item.Status, &item.CreatedAt, &item.HasCompletedRun, &latest)
	if latest.Valid {
		state := domain.RunState(latest.String)
		item.LatestRunState = &state
	}
	return item, err
}

func (repository *CatalogRepository) GetDataset(ctx context.Context, id string) (domain.Dataset, error) {
	item, err := scanDataset(repository.database.QueryRowContext(ctx, `SELECT `+datasetColumns+`
		FROM datasets d LEFT JOIN LATERAL (SELECT state FROM replay_runs WHERE dataset_id=d.id ORDER BY created_at DESC,id DESC LIMIT 1) latest ON true
		WHERE d.id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return item, application.ErrNotFound
	}
	if err != nil {
		return item, fmt.Errorf("get dataset: %w", err)
	}
	return item, nil
}

func (repository *CatalogRepository) ListDatasets(ctx context.Context, limit int, cursor string) (application.DatasetPage, error) {
	var cursorTime time.Time
	var cursorID string
	var err error
	if cursor != "" {
		cursorTime, cursorID, err = decodeCursor(cursor)
		if err != nil {
			return application.DatasetPage{}, err
		}
	}
	rows, err := repository.database.QueryContext(ctx, `SELECT `+datasetColumns+`
		FROM datasets d LEFT JOIN LATERAL (SELECT state FROM replay_runs WHERE dataset_id=d.id ORDER BY created_at DESC,id DESC LIMIT 1) latest ON true
		WHERE ($1::timestamptz IS NULL OR (d.created_at,d.id) < ($1,$2::uuid))
		ORDER BY d.created_at DESC,d.id DESC LIMIT $3`, nullableTime(cursorTime), nullableString(cursorID), limit+1)
	if err != nil {
		return application.DatasetPage{}, fmt.Errorf("list datasets: %w", err)
	}
	defer rows.Close()
	page := application.DatasetPage{Items: make([]domain.Dataset, 0, limit)}
	for rows.Next() {
		item, scanErr := scanDataset(rows)
		if scanErr != nil {
			return page, scanErr
		}
		page.Items = append(page.Items, item)
	}
	if err = rows.Err(); err != nil {
		return page, err
	}
	if len(page.Items) > limit {
		last := page.Items[limit-1]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
		page.Items = page.Items[:limit]
	}
	return page, nil
}

func (repository *CatalogRepository) CreateRun(ctx context.Context, datasetID string) (domain.ReplayRun, error) {
	if _, err := repository.GetDataset(ctx, datasetID); err != nil {
		return domain.ReplayRun{}, err
	}
	var runID string
	err := repository.database.QueryRowContext(ctx, `INSERT INTO replay_runs (dataset_id,state,started_at) VALUES ($1,'RUNNING',now()) RETURNING id`, datasetID).Scan(&runID)
	if err != nil {
		return domain.ReplayRun{}, fmt.Errorf("create run: %w", err)
	}
	return repository.GetRun(ctx, runID)
}

func scanRun(scanner interface{ Scan(...any) error }) (domain.ReplayRun, error) {
	var item domain.ReplayRun
	var started, finished sql.NullTime
	err := scanner.Scan(&item.ID, &item.DatasetID, &item.State, &started, &finished, &item.InputCount, &item.AcceptedCount, &item.DuplicateCount, &item.RejectedCount, &item.CreatedAt)
	if started.Valid {
		item.StartedAt = &started.Time
	}
	if finished.Valid {
		item.FinishedAt = &finished.Time
	}
	return item, err
}

const runSelect = `SELECT r.id,r.dataset_id,r.state,r.started_at,r.finished_at,
	COUNT(o.event_id),COUNT(*) FILTER (WHERE o.outcome='ACCEPTED'),COUNT(*) FILTER (WHERE o.outcome='DUPLICATE'),COUNT(*) FILTER (WHERE o.outcome='REJECTED'),r.created_at
	FROM replay_runs r LEFT JOIN run_event_outcomes o ON o.run_id=r.id WHERE r.id=$1 GROUP BY r.id`

func (repository *CatalogRepository) ListRuns(ctx context.Context, datasetID string, state domain.RunState, limit int, cursor string) (application.RunPage, error) {
	var cursorTime time.Time
	var cursorID string
	var err error
	if cursor != "" {
		cursorTime, cursorID, err = decodeCursor(cursor)
		if err != nil {
			return application.RunPage{}, err
		}
	}
	rows, err := repository.database.QueryContext(ctx, `
		SELECT r.id,r.dataset_id,r.state,r.started_at,r.finished_at,
			COUNT(o.event_id),COUNT(*) FILTER (WHERE o.outcome='ACCEPTED'),COUNT(*) FILTER (WHERE o.outcome='DUPLICATE'),COUNT(*) FILTER (WHERE o.outcome='REJECTED'),r.created_at
		FROM replay_runs r LEFT JOIN run_event_outcomes o ON o.run_id=r.id
		WHERE ($1::uuid IS NULL OR r.dataset_id=$1) AND ($2::text IS NULL OR r.state=$2)
			AND ($3::timestamptz IS NULL OR (r.created_at,r.id)<($3,$4::uuid))
		GROUP BY r.id ORDER BY r.created_at DESC,r.id DESC LIMIT $5`,
		nullableString(datasetID), nullableString(string(state)), nullableTime(cursorTime), nullableString(cursorID), limit+1)
	if err != nil {
		return application.RunPage{}, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()
	page := application.RunPage{Items: make([]domain.ReplayRun, 0, limit)}
	for rows.Next() {
		item, scanErr := scanRun(rows)
		if scanErr != nil {
			return page, scanErr
		}
		page.Items = append(page.Items, item)
	}
	if err = rows.Err(); err != nil {
		return page, err
	}
	if len(page.Items) > limit {
		last := page.Items[limit-1]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
		page.Items = page.Items[:limit]
	}
	return page, nil
}

func (repository *CatalogRepository) GetRun(ctx context.Context, id string) (domain.ReplayRun, error) {
	item, err := scanRun(repository.database.QueryRowContext(ctx, runSelect, id))
	if errors.Is(err, sql.ErrNoRows) {
		return item, application.ErrNotFound
	}
	if err != nil {
		return item, fmt.Errorf("get run: %w", err)
	}
	return item, nil
}

func (repository *CatalogRepository) CompleteRun(ctx context.Context, id string, expected int64) (domain.ReplayRun, error) {
	tx, err := repository.database.BeginTx(ctx, nil)
	if err != nil {
		return domain.ReplayRun{}, err
	}
	defer tx.Rollback()
	var state domain.RunState
	if err = tx.QueryRowContext(ctx, `SELECT state FROM replay_runs WHERE id=$1 FOR UPDATE`, id).Scan(&state); errors.Is(err, sql.ErrNoRows) {
		return domain.ReplayRun{}, application.ErrNotFound
	} else if err != nil {
		return domain.ReplayRun{}, err
	}
	if state != domain.RunRunning {
		return domain.ReplayRun{}, fmt.Errorf("%w: run is %s", application.ErrConflict, state)
	}
	var total, accepted, duplicate, rejected int64
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE outcome='ACCEPTED'),COUNT(*) FILTER (WHERE outcome='DUPLICATE'),COUNT(*) FILTER (WHERE outcome='REJECTED') FROM run_event_outcomes WHERE run_id=$1`, id).Scan(&total, &accepted, &duplicate, &rejected)
	if err != nil {
		return domain.ReplayRun{}, err
	}
	if total != expected {
		return domain.ReplayRun{}, fmt.Errorf("%w: expected %d durable outcomes, found %d", application.ErrConflict, expected, total)
	}
	_, err = tx.ExecContext(ctx, `UPDATE replay_runs SET state='COMPLETED',finished_at=now(),input_count=$2,accepted_count=$3,duplicate_count=$4,rejected_count=$5 WHERE id=$1`, id, total, accepted, duplicate, rejected)
	if err != nil {
		return domain.ReplayRun{}, err
	}
	if err = tx.Commit(); err != nil {
		return domain.ReplayRun{}, err
	}
	return repository.GetRun(ctx, id)
}

func (repository *CatalogRepository) CancelRun(ctx context.Context, id string) (domain.ReplayRun, error) {
	result, err := repository.database.ExecContext(ctx, `UPDATE replay_runs SET state='CANCELLED',finished_at=now() WHERE id=$1 AND state IN ('CREATED','RUNNING')`, id)
	if err != nil {
		return domain.ReplayRun{}, err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		var exists bool
		_ = repository.database.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM replay_runs WHERE id=$1)`, id).Scan(&exists)
		if !exists {
			return domain.ReplayRun{}, application.ErrNotFound
		}
		return domain.ReplayRun{}, application.ErrConflict
	}
	return repository.GetRun(ctx, id)
}

func (repository *CatalogRepository) ListHourlyZoneStats(ctx context.Context, datasetID string, start, end time.Time, zone *int32, limit int) ([]domain.HourlyZoneStat, bool, error) {
	if _, err := repository.GetDataset(ctx, datasetID); err != nil {
		return nil, false, err
	}
	rows, err := repository.database.QueryContext(ctx, `SELECT dataset_id,pickup_zone_id,pickup_hour_utc,trip_count,total_distance_milli_miles,total_fare_cents,fare_observed_count FROM hourly_zone_stats WHERE dataset_id=$1 AND pickup_hour_utc >= $2 AND pickup_hour_utc < $3 AND ($4::integer IS NULL OR pickup_zone_id=$4) ORDER BY pickup_hour_utc,pickup_zone_id LIMIT $5`, datasetID, start, end, zone, limit)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	items := make([]domain.HourlyZoneStat, 0)
	for rows.Next() {
		var item domain.HourlyZoneStat
		if err = rows.Scan(&item.DatasetID, &item.PickupZoneID, &item.PickupHourUTC, &item.TripCount, &item.TotalDistanceMilliMiles, &item.TotalFareCents, &item.FareObservedCount); err != nil {
			return nil, false, err
		}
		items = append(items, item)
	}
	var complete bool
	err = repository.database.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM replay_runs WHERE dataset_id=$1 AND state='COMPLETED')`, datasetID).Scan(&complete)
	return items, complete, err
}

func (repository *CatalogRepository) ListRejections(ctx context.Context, datasetID string, limit int, cursor string) (application.RejectionPage, error) {
	if _, err := repository.GetDataset(ctx, datasetID); err != nil {
		return application.RejectionPage{}, err
	}
	var cursorTime time.Time
	var cursorKey string
	var err error
	if cursor != "" {
		cursorTime, cursorKey, err = decodeCursor(cursor)
		if err != nil {
			return application.RejectionPage{}, err
		}
	}
	rows, err := repository.database.QueryContext(ctx, `SELECT dataset_id,event_id,source_row_number,reason_code,detail,validation_policy_version,rejected_at FROM rejected_events WHERE dataset_id=$1 AND ($2::timestamptz IS NULL OR (rejected_at,event_id||':'||validation_policy_version)<($2,$3)) ORDER BY rejected_at DESC,event_id DESC,validation_policy_version DESC LIMIT $4`, datasetID, nullableTime(cursorTime), nullableString(cursorKey), limit+1)
	if err != nil {
		return application.RejectionPage{}, err
	}
	defer rows.Close()
	page := application.RejectionPage{Items: make([]domain.RejectionRecord, 0, limit)}
	for rows.Next() {
		var item domain.RejectionRecord
		if err = rows.Scan(&item.DatasetID, &item.EventID, &item.SourceRowNumber, &item.ReasonCode, &item.Detail, &item.ValidationPolicyVersion, &item.RejectedAt); err != nil {
			return page, err
		}
		page.Items = append(page.Items, item)
	}
	if len(page.Items) > limit {
		last := page.Items[limit-1]
		page.NextCursor = encodeCursor(last.RejectedAt, last.EventID+":"+last.ValidationPolicyVersion)
		page.Items = page.Items[:limit]
	}
	return page, rows.Err()
}

func encodeCursor(at time.Time, key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(at.UTC().Format(time.RFC3339Nano) + "|" + key))
}
func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	at, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid cursor")
	}
	return at, parts[1], nil
}
func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}
func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
