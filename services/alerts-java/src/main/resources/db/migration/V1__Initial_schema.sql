CREATE TABLE alert_rules (
    rule_id VARCHAR(255) NOT NULL,
    rule_version INTEGER NOT NULL,
    kind VARCHAR(50) NOT NULL,
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rule_id, rule_version)
);

CREATE TABLE alerts (
    alert_id VARCHAR(64) PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    rule_id VARCHAR(255) NOT NULL,
    rule_version INTEGER NOT NULL,
    dataset_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    FOREIGN KEY (rule_id, rule_version) REFERENCES alert_rules(rule_id, rule_version)
);

CREATE INDEX idx_alerts_event_id ON alerts(event_id);
CREATE INDEX idx_alerts_dataset_id ON alerts(dataset_id);

CREATE TABLE alert_event_outcomes (
    consumer_group VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INTEGER NOT NULL,
    offset_num BIGINT NOT NULL,
    outcome VARCHAR(50) NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition, offset_num)
);

CREATE TABLE alert_consumer_failures (
    consumer_group VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INTEGER NOT NULL,
    offset_num BIGINT NOT NULL,
    message_key VARCHAR(255),
    payload_sha256 VARCHAR(64),
    reason TEXT NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition, offset_num)
);
