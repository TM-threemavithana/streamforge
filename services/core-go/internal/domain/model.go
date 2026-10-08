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
	ID                  string    `json:"id"`
	SourceType          string    `json:"source_type"`
	SourceSHA256        string    `json:"source_sha256"`
	SourceSchemaVersion string    `json:"source_schema_version"`
	Filename            string    `json:"filename"`
	SourceSizeBytes     int64     `json:"source_size_bytes"`
	Status              string    `json:"status"`
	CreatedAt           time.Time `json:"created_at"`
	HasCompletedRun     bool      `json:"has_completed_run"`
	LatestRunState      *RunState `json:"latest_run_state,omitempty"`
}

type ReplayRun struct {
	ID             string     `json:"id"`
	DatasetID      string     `json:"dataset_id"`
	State          RunState   `json:"state"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	InputCount     int64      `json:"input_count"`
	AcceptedCount  int64      `json:"accepted_count"`
	DuplicateCount int64      `json:"duplicate_count"`
	RejectedCount  int64      `json:"rejected_count"`
	CreatedAt      time.Time  `json:"created_at"`
}

type HourlyZoneStat struct {
	DatasetID               string    `json:"dataset_id"`
	PickupZoneID            int32     `json:"pickup_zone_id"`
	PickupHourUTC           time.Time `json:"pickup_hour_utc"`
	TripCount               int64     `json:"trip_count"`
	TotalDistanceMilliMiles int64     `json:"total_distance_milli_miles"`
	TotalFareCents          int64     `json:"total_fare_cents"`
	FareObservedCount       int64     `json:"fare_observed_count"`
}

type RejectionRecord struct {
	DatasetID               string    `json:"dataset_id"`
	EventID                 string    `json:"event_id"`
	SourceRowNumber         uint64    `json:"source_row_number"`
	ReasonCode              string    `json:"reason_code"`
	Detail                  string    `json:"detail"`
	ValidationPolicyVersion string    `json:"validation_policy_version"`
	RejectedAt              time.Time `json:"rejected_at"`
}

type TripEvent struct {
	DatasetID          string
	RunID              string
	EventID            string
	SourceSHA256       string
	SourceRowNumber    uint64
	PickupAt           time.Time
	DropoffAt          time.Time
	PickupZoneID       int32
	DropoffZoneID      *int32
	DistanceMilliMiles int64
	FareCents          *int64
}

type SourceRejection struct {
	DatasetID               string
	RunID                   string
	EventID                 string
	SourceRowNumber         uint64
	ReasonCode              string
	Detail                  string
	ValidationPolicyVersion string
}

type EventResult struct {
	EventID    string
	Outcome    Outcome
	AckStage   string
	ReasonCode string
}
