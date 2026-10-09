INSERT INTO alert_rules (rule_id, rule_version, kind, parameters, enabled, created_at)
VALUES 
    ('high_fare', 1, 'HIGH_FARE', '{"threshold_cents": 10000}'::jsonb, true, NOW()),
    ('long_distance', 1, 'LONG_DISTANCE', '{"threshold_milli_miles": 50000}'::jsonb, true, NOW()),
    ('unusual_duration', 1, 'UNUSUAL_DURATION', '{"max_duration_seconds": 10800}'::jsonb, true, NOW())
ON CONFLICT (rule_id, rule_version) DO NOTHING;
