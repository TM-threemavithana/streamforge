CREATE TABLE hourly_zone_stats (
    dataset_id uuid NOT NULL REFERENCES datasets(id),
    pickup_zone_id integer NOT NULL CHECK (pickup_zone_id > 0),
    pickup_hour_utc timestamptz NOT NULL,
    trip_count bigint NOT NULL DEFAULT 0 CHECK (trip_count >= 0),
    total_distance_milli_miles bigint NOT NULL DEFAULT 0 CHECK (total_distance_milli_miles >= 0),
    total_fare_cents bigint NOT NULL DEFAULT 0,
    fare_observed_count bigint NOT NULL DEFAULT 0 CHECK (fare_observed_count >= 0),
    PRIMARY KEY (dataset_id, pickup_zone_id, pickup_hour_utc),
    CHECK (pickup_hour_utc = date_trunc('hour', pickup_hour_utc)),
    CHECK (fare_observed_count <= trip_count)
);

