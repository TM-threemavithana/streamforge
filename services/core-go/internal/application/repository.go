package application

import (
	"context"

	"github.com/example/streamforge/services/core-go/internal/domain"
)

// EventRepository owns the durable idempotency boundary. Implementations must
// insert a unique event, update its hourly aggregate, and record the run outcome
// in one database transaction before returning DATABASE_COMMITTED.
type EventRepository interface {
	ProcessEvent(ctx context.Context, event domain.TripEvent) (domain.EventResult, error)
}
