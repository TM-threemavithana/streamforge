CREATE TABLE trip_events (
    dataset_id uuid NOT NULL REFERENCES datasets(id),
    event_id char(64) NOT NULL CHECK (event_id ~ '^[0-9a-f]{64}$'),
    source_row_number bigint NOT NULL CHECK (source_row_number >= 0),
    pickup_at timestamptz NOT NULL,
    dropoff_at timestamptz NOT NULL,
    pickup_zone_id integer NOT NULL CHECK (pickup_zone_id > 0),
    dropoff_zone_id integer CHECK (dropoff_zone_id > 0),
    distance_milli_miles bigint NOT NULL CHECK (distance_milli_miles >= 0),
    fare_cents bigint,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, event_id),
    UNIQUE (dataset_id, source_row_number),
    CHECK (dropoff_at >= pickup_at)
);

CREATE INDEX trip_events_dataset_pickup_idx ON trip_events (dataset_id, pickup_at);

