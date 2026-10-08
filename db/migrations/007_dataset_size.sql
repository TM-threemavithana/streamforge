ALTER TABLE datasets
    ADD COLUMN source_size_bytes bigint NOT NULL DEFAULT 0
    CHECK (source_size_bytes >= 0);

ALTER TABLE datasets
    ALTER COLUMN source_size_bytes DROP DEFAULT;
