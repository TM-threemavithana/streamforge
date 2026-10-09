package eventstream

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/example/streamforge/services/core-go/internal/domain"
)

const (
	SchemaVersionV1 = "streamforge.raw-event:v1"
	KindTrip        = "TRIP"
	KindRejection   = "SOURCE_REJECTION"
)

type Envelope struct {
	SchemaVersion string                  `json:"schema_version"`
	Kind          string                  `json:"kind"`
	Trip          *domain.TripEvent       `json:"trip,omitempty"`
	Rejection     *domain.SourceRejection `json:"rejection,omitempty"`
}

func EncodeTrip(event domain.TripEvent) ([]byte, error) {
	return json.Marshal(Envelope{SchemaVersion: SchemaVersionV1, Kind: KindTrip, Trip: &event})
}

func EncodeRejection(rejection domain.SourceRejection) ([]byte, error) {
	return json.Marshal(Envelope{SchemaVersion: SchemaVersionV1, Kind: KindRejection, Rejection: &rejection})
}

func Decode(payload []byte) (Envelope, error) {
	var envelope Envelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return envelope, fmt.Errorf("decode JSON envelope: %w", err)
	}
	if envelope.SchemaVersion != SchemaVersionV1 {
		return envelope, fmt.Errorf("unsupported schema_version %q", envelope.SchemaVersion)
	}
	switch envelope.Kind {
	case KindTrip:
		if envelope.Trip == nil || envelope.Rejection != nil {
			return envelope, errors.New("TRIP envelope must contain only trip")
		}
	case KindRejection:
		if envelope.Rejection == nil || envelope.Trip != nil {
			return envelope, errors.New("SOURCE_REJECTION envelope must contain only rejection")
		}
	default:
		return envelope, fmt.Errorf("unsupported event kind %q", envelope.Kind)
	}
	return envelope, nil
}
