package postgres

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
)

const databaseCommitted = "DATABASE_COMMITTED"

type row interface {
	Scan(dest ...any) error
}

type transaction interface {
	Exec(ctx context.Context, sql string, arguments ...any) error
	QueryRow(ctx context.Context, sql string, arguments ...any) row
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type beginTransaction func(context.Context) (transaction, error)

type EventRepository struct {
	begin beginTransaction
}

func NewEventRepository(database *sql.DB) *EventRepository {
	return &EventRepository{begin: func(ctx context.Context) (transaction, error) {
		tx, err := database.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		return sqlTransaction{Tx: tx}, nil
	}}
}

type sqlTransaction struct {
	*sql.Tx
}

func (tx sqlTransaction) Exec(ctx context.Context, statement string, arguments ...any) error {
	_, err := tx.Tx.ExecContext(ctx, statement, arguments...)
	return err
}

func (tx sqlTransaction) QueryRow(ctx context.Context, statement string, arguments ...any) row {
	return tx.Tx.QueryRowContext(ctx, statement, arguments...)
}

func (tx sqlTransaction) Commit(context.Context) error {
	return tx.Tx.Commit()
}

func (tx sqlTransaction) Rollback(context.Context) error {
	return tx.Tx.Rollback()
}

func (repository *EventRepository) ProcessEvent(ctx context.Context, event domain.TripEvent) (result domain.EventResult, err error) {
	if err := validateEvent(event); err != nil {
		return result, err
	}

	tx, err := repository.begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin event transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// A response may have been lost after commit. Reusing the same run/event ID
	// must return the original durable outcome rather than changing its meaning.
	result, found, err := existingOutcome(ctx, tx, event.RunID, event.EventID)
	if err != nil {
		return result, err
	}
	if found {
		if err = tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit outcome lookup: %w", err)
		}
		return result, nil
	}

	if err = verifyRunnable(ctx, tx, event.RunID, event.DatasetID); err != nil {
		return result, err
	}
	if err = verifyDatasetSource(ctx, tx, event.DatasetID, event.SourceSHA256); err != nil {
		return result, err
	}
	claimed, err := claimOutcome(ctx, tx, event.RunID, event.EventID, domain.OutcomeAccepted, "")
	if err != nil {
		return result, err
	}
	if !claimed {
		result, found, err = existingOutcome(ctx, tx, event.RunID, event.EventID)
		if err != nil {
			return result, err
		}
		if !found {
			return result, errors.New("run outcome conflict occurred but no durable outcome exists")
		}
		if err = tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit competing outcome lookup: %w", err)
		}
		return result, nil
	}

	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO trip_events (
			dataset_id, event_id, source_row_number, pickup_at, dropoff_at,
			pickup_zone_id, dropoff_zone_id, distance_milli_miles, fare_cents
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (dataset_id, event_id) DO NOTHING
		RETURNING event_id`,
		event.DatasetID, event.EventID, event.SourceRowNumber, event.PickupAt,
		event.DropoffAt, event.PickupZoneID, event.DropoffZoneID,
		event.DistanceMilliMiles, event.FareCents,
	).Scan(&insertedID)

	outcome := domain.OutcomeAccepted
	if errors.Is(err, sql.ErrNoRows) {
		outcome = domain.OutcomeDuplicate
		if err = tx.Exec(ctx, `UPDATE run_event_outcomes SET outcome = 'DUPLICATE' WHERE run_id = $1 AND event_id = $2`, event.RunID, event.EventID); err != nil {
			return result, fmt.Errorf("update duplicate outcome: %w", err)
		}
	} else if err != nil {
		return result, fmt.Errorf("insert trip event: %w", err)
	} else if err = updateAggregate(ctx, tx, event); err != nil {
		return result, err
	}
	result = domain.EventResult{EventID: event.EventID, Outcome: outcome, AckStage: databaseCommitted}

	if err = tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit event transaction: %w", err)
	}
	return result, nil
}

func (repository *EventRepository) ReportSourceRejection(ctx context.Context, rejection domain.SourceRejection) (result domain.EventResult, err error) {
	if err := validateSourceRejection(rejection); err != nil {
		return result, err
	}

	tx, err := repository.begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin rejection transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	result, found, err := existingOutcome(ctx, tx, rejection.RunID, rejection.EventID)
	if err != nil {
		return result, err
	}
	if found {
		if err = tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit rejection outcome lookup: %w", err)
		}
		return result, nil
	}

	if err = verifyRunnable(ctx, tx, rejection.RunID, rejection.DatasetID); err != nil {
		return result, err
	}
	claimed, err := claimOutcome(ctx, tx, rejection.RunID, rejection.EventID, domain.OutcomeRejected, rejection.ReasonCode)
	if err != nil {
		return result, err
	}
	if !claimed {
		result, found, err = existingOutcome(ctx, tx, rejection.RunID, rejection.EventID)
		if err != nil {
			return result, err
		}
		if !found {
			return result, errors.New("rejection outcome conflict occurred but no durable outcome exists")
		}
		if err = tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit competing rejection lookup: %w", err)
		}
		return result, nil
	}

	err = tx.Exec(ctx, `
		INSERT INTO rejected_events (
			dataset_id, event_id, source_row_number, reason_code, detail,
			validation_policy_version
		) VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (dataset_id, event_id, validation_policy_version) DO NOTHING`,
		rejection.DatasetID, rejection.EventID, rejection.SourceRowNumber,
		rejection.ReasonCode, rejection.Detail, rejection.ValidationPolicyVersion,
	)
	if err != nil {
		return result, fmt.Errorf("insert rejected event: %w", err)
	}

	result = domain.EventResult{
		EventID: rejection.EventID, Outcome: domain.OutcomeRejected,
		AckStage: databaseCommitted, ReasonCode: rejection.ReasonCode,
	}

	if err = tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit rejection transaction: %w", err)
	}
	return result, nil
}

func verifyRunnable(ctx context.Context, tx transaction, runID, datasetID string) error {
	var state string
	err := tx.QueryRow(ctx, `
		SELECT state FROM replay_runs
		WHERE id = $1 AND dataset_id = $2
		FOR SHARE`, runID, datasetID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s is not RUNNING for dataset %s", domain.ErrRunNotRunning, runID, datasetID)
	}
	if err != nil {
		return fmt.Errorf("verify replay run: %w", err)
	}
	if state != string(domain.RunRunning) {
		return fmt.Errorf("%w: %s is not RUNNING for dataset %s", domain.ErrRunNotRunning, runID, datasetID)
	}
	return nil
}

func claimOutcome(ctx context.Context, tx transaction, runID, eventID string, outcome domain.Outcome, reasonCode string) (bool, error) {
	var claimedID string
	var reason any
	if reasonCode != "" {
		reason = reasonCode
	}
	err := tx.QueryRow(ctx, `
		INSERT INTO run_event_outcomes (run_id, event_id, outcome, reason_code)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (run_id, event_id) DO NOTHING
		RETURNING event_id`, runID, eventID, outcome, reason).Scan(&claimedID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim run outcome: %w", err)
	}
	return true, nil
}

func verifyDatasetSource(ctx context.Context, tx transaction, datasetID, sourceSHA256 string) error {
	var matches bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM datasets WHERE id = $1 AND source_sha256 = $2
		)`, datasetID, sourceSHA256).Scan(&matches)
	if err != nil {
		return fmt.Errorf("verify dataset source: %w", err)
	}
	if !matches {
		return fmt.Errorf("%w: source SHA-256 does not match dataset %s", domain.ErrInvalidInput, datasetID)
	}
	return nil
}

func existingOutcome(ctx context.Context, tx transaction, runID, eventID string) (domain.EventResult, bool, error) {
	var outcome string
	var reasonCode string
	err := tx.QueryRow(ctx, `
		SELECT outcome, COALESCE(reason_code, '')
		FROM run_event_outcomes WHERE run_id = $1 AND event_id = $2`,
		runID, eventID,
	).Scan(&outcome, &reasonCode)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.EventResult{}, false, nil
	}
	if err != nil {
		return domain.EventResult{}, false, fmt.Errorf("read existing run outcome: %w", err)
	}
	return domain.EventResult{
		EventID: eventID, Outcome: domain.Outcome(outcome),
		AckStage: databaseCommitted, ReasonCode: reasonCode,
	}, true, nil
}

func updateAggregate(ctx context.Context, tx transaction, event domain.TripEvent) error {
	fare := int64(0)
	fareCount := int64(0)
	if event.FareCents != nil {
		fare = *event.FareCents
		fareCount = 1
	}
	err := tx.Exec(ctx, `
		INSERT INTO hourly_zone_stats (
			dataset_id, pickup_zone_id, pickup_hour_utc, trip_count,
			total_distance_milli_miles, total_fare_cents, fare_observed_count
		) VALUES ($1,$2,$3,1,$4,$5,$6)
		ON CONFLICT (dataset_id, pickup_zone_id, pickup_hour_utc)
		DO UPDATE SET
			trip_count = hourly_zone_stats.trip_count + 1,
			total_distance_milli_miles = hourly_zone_stats.total_distance_milli_miles + EXCLUDED.total_distance_milli_miles,
			total_fare_cents = hourly_zone_stats.total_fare_cents + EXCLUDED.total_fare_cents,
			fare_observed_count = hourly_zone_stats.fare_observed_count + EXCLUDED.fare_observed_count`,
		event.DatasetID, event.PickupZoneID, event.PickupAt.UTC().Truncate(time.Hour),
		event.DistanceMilliMiles, fare, fareCount,
	)
	if err != nil {
		return fmt.Errorf("update hourly aggregate: %w", err)
	}
	return nil
}

func validateEvent(event domain.TripEvent) error {
	if event.DatasetID == "" || event.RunID == "" || !isLowerHexSHA256(event.EventID) {
		return fmt.Errorf("%w: dataset ID, run ID, and lowercase hexadecimal event ID are required", domain.ErrInvalidInput)
	}
	if !isLowerHexSHA256(event.SourceSHA256) {
		return fmt.Errorf("%w: lowercase hexadecimal source SHA-256 is required", domain.ErrInvalidInput)
	}
	if event.PickupAt.IsZero() || event.DropoffAt.Before(event.PickupAt) {
		return fmt.Errorf("%w: event timestamps are invalid", domain.ErrInvalidInput)
	}
	if event.PickupZoneID <= 0 || event.DistanceMilliMiles < 0 {
		return fmt.Errorf("%w: event zone or distance is invalid", domain.ErrInvalidInput)
	}
	return nil
}

func isLowerHexSHA256(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validateSourceRejection(rejection domain.SourceRejection) error {
	if rejection.DatasetID == "" || rejection.RunID == "" || !isLowerHexSHA256(rejection.EventID) {
		return fmt.Errorf("%w: dataset ID, run ID, and lowercase hexadecimal event ID are required", domain.ErrInvalidInput)
	}
	if rejection.ReasonCode == "" || rejection.ValidationPolicyVersion == "" {
		return fmt.Errorf("%w: rejection reason and validation policy version are required", domain.ErrInvalidInput)
	}
	return nil
}
