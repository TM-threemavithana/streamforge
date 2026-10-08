CREATE TABLE replay_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id uuid NOT NULL REFERENCES datasets(id),
    state text NOT NULL DEFAULT 'CREATED' CHECK (state IN ('CREATED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    started_at timestamptz,
    finished_at timestamptz,
    input_count bigint NOT NULL DEFAULT 0 CHECK (input_count >= 0),
    accepted_count bigint NOT NULL DEFAULT 0 CHECK (accepted_count >= 0),
    duplicate_count bigint NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
    rejected_count bigint NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (finished_at IS NULL OR started_at IS NOT NULL),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE INDEX replay_runs_dataset_created_idx ON replay_runs (dataset_id, created_at DESC);

