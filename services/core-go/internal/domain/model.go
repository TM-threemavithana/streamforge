package domain

import "time"

type RunState string

const (
	RunCreated   RunState = "CREATED"
	RunRunning   RunState = "RUNNING"
	RunCompleted RunState = "COMPLETED"
	RunFailed    RunState = "FAILED"
	RunCancelled RunState = "CANCELLED"
)

type Outcome string

const (
	OutcomeAccepted  Outcome = "ACCEPTED"
	OutcomeDuplicate Outcome = "DUPLICATE"
	OutcomeRejected  Outcome = "REJECTED"
)

type Dataset struct {
	ID                  string
	SourceType          string
	SourceSHA256        string
	SourceSchemaVersion string
	Filename            string
}

type ReplayRun struct {
	ID        string
	DatasetID string
	State     RunState
}

type TripEvent struct {
	DatasetID          string
	RunID              string
	EventID            string
	SourceRowNumber    uint64
	PickupAt           time.Time
	DropoffAt          time.Time
	PickupZoneID       int32
	DropoffZoneID      *int32
	DistanceMilliMiles int64
	FareCents          *int64
}

type EventResult struct {
	EventID    string
	Outcome    Outcome
	AckStage   string
	ReasonCode string
}
