CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE datasets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type text NOT NULL,
    source_sha256 char(64) NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    source_schema_version text NOT NULL,
    filename text NOT NULL,
    status text NOT NULL DEFAULT 'REGISTERED' CHECK (status IN ('REGISTERED', 'UNSUPPORTED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_type, source_sha256)
);

