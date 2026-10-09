package application

import (
	"context"

	"github.com/example/streamforge/services/core-go/internal/domain"
)

type ConsumerLagReader interface {
	ReadConsumerLag(context.Context) (domain.ConsumerLag, error)
}
