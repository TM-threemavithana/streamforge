CREATE TABLE rejected_events (
    dataset_id uuid NOT NULL REFERENCES datasets(id),
    event_id char(64) NOT NULL CHECK (event_id ~ '^[0-9a-f]{64}$'),
    source_row_number bigint NOT NULL CHECK (source_row_number >= 0),
    reason_code text NOT NULL,
    detail text NOT NULL DEFAULT '',
    validation_policy_version text NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, event_id, validation_policy_version)
);

CREATE INDEX rejected_events_dataset_reason_idx ON rejected_events (dataset_id, reason_code, rejected_at);

