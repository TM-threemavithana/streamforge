CREATE TABLE kafka_consumer_failures (
    consumer_group text NOT NULL,
    topic text NOT NULL,
    partition_id integer NOT NULL CHECK (partition_id >= 0),
    offset_id bigint NOT NULL CHECK (offset_id >= 0),
    message_key text,
    payload_sha256 char(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    reason text NOT NULL,
    failed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition_id, offset_id)
);

CREATE INDEX kafka_consumer_failures_failed_at_idx
    ON kafka_consumer_failures (failed_at DESC);
