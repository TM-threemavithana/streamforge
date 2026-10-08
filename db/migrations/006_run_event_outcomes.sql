CREATE TABLE run_event_outcomes (
    run_id uuid NOT NULL REFERENCES replay_runs(id),
    event_id char(64) NOT NULL CHECK (event_id ~ '^[0-9a-f]{64}$'),
    outcome text NOT NULL CHECK (outcome IN ('ACCEPTED', 'DUPLICATE', 'REJECTED')),
    reason_code text,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, event_id),
    CHECK ((outcome = 'REJECTED') OR reason_code IS NULL)
);

CREATE INDEX run_event_outcomes_run_outcome_idx ON run_event_outcomes (run_id, outcome);

