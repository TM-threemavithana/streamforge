package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

	var runnable bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM replay_runs
			WHERE id = $1 AND dataset_id = $2 AND state = 'RUNNING'
		)`, event.RunID, event.DatasetID).Scan(&runnable)
	if err != nil {
		return result, fmt.Errorf("verify replay run: %w", err)
	}
	if !runnable {
		return result, fmt.Errorf("replay run %s is not RUNNING for dataset %s", event.RunID, event.DatasetID)
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
	} else if err != nil {
		return result, fmt.Errorf("insert trip event: %w", err)
	} else if err = updateAggregate(ctx, tx, event); err != nil {
		return result, err
	}

	// A concurrent retry in the same run may win this key. If so, read and
	// return its original outcome so all retries receive stable semantics.
	var durableOutcome string
	var reasonCode string
	err = tx.QueryRow(ctx, `
		INSERT INTO run_event_outcomes (run_id, event_id, outcome)
		VALUES ($1, $2, $3)
		ON CONFLICT (run_id, event_id) DO NOTHING
		RETURNING outcome, COALESCE(reason_code, '')`,
		event.RunID, event.EventID, outcome,
	).Scan(&durableOutcome, &reasonCode)
	if errors.Is(err, sql.ErrNoRows) {
		result, found, err = existingOutcome(ctx, tx, event.RunID, event.EventID)
		if err != nil {
			return result, err
		}
		if !found {
			return result, errors.New("run outcome conflict occurred but no durable outcome exists")
		}
	} else if err != nil {
		return result, fmt.Errorf("record run outcome: %w", err)
	} else {
		result = domain.EventResult{
			EventID: event.EventID, Outcome: domain.Outcome(durableOutcome),
			AckStage: databaseCommitted, ReasonCode: reasonCode,
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit event transaction: %w", err)
	}
	return result, nil
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
	if event.DatasetID == "" || event.RunID == "" || len(event.EventID) != 64 {
		return errors.New("dataset ID, run ID, and 64-character event ID are required")
	}
	if event.PickupAt.IsZero() || event.DropoffAt.Before(event.PickupAt) {
		return errors.New("event timestamps are invalid")
	}
	if event.PickupZoneID <= 0 || event.DistanceMilliMiles < 0 {
		return errors.New("event zone or distance is invalid")
	}
	return nil
}
