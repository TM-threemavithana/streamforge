package com.example.streamforge.alerts.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class RuleEngine {

    public Optional<Alert> evaluate(TripEvent event, AlertRule rule) {
        if (!rule.enabled()) {
            return Optional.empty();
        }

        boolean isAnomaly = switch (rule.kind()) {
            case HIGH_FARE -> evaluateHighFare(event, rule);
            case LONG_DISTANCE -> evaluateLongDistance(event, rule);
            case UNUSUAL_DURATION -> evaluateUnusualDuration(event, rule);
        };

        if (isAnomaly) {
            String alertId = Alert.generateAlertId(event.eventId(), rule.ruleId(), rule.ruleVersion());
            return Optional.of(new Alert(
                    alertId,
                    event.eventId(),
                    rule.ruleId(),
                    rule.ruleVersion(),
                    event.datasetId(),
                    buildPayload(rule),
                    Instant.now()
            ));
        }

        return Optional.empty();
    }

    private boolean evaluateHighFare(TripEvent event, AlertRule rule) {
        if (event.fareCents() == null) return false;
        long threshold = getParameter(rule, "threshold_cents", 10000L); // Default $100.00
        return event.fareCents() > threshold;
    }

    private boolean evaluateLongDistance(TripEvent event, AlertRule rule) {
        long threshold = getParameter(rule, "threshold_milli_miles", 50000L); // Default 50 miles
        return event.distanceMilliMiles() > threshold;
    }

    private boolean evaluateUnusualDuration(TripEvent event, AlertRule rule) {
        if (event.pickupAt() == null || event.dropoffAt() == null) return false;
        long durationSeconds = Duration.between(event.pickupAt(), event.dropoffAt()).getSeconds();
        long maxThreshold = getParameter(rule, "max_duration_seconds", 10800L); // Default 3 hours
        return durationSeconds < 0 || durationSeconds > maxThreshold;
    }

    private long getParameter(AlertRule rule, String key, long defaultValue) {
        if (rule.parameters() != null && rule.parameters().containsKey(key)) {
            Object val = rule.parameters().get(key);
            if (val instanceof Number n) {
                return n.longValue();
            }
        }
        return defaultValue;
    }

    private String buildPayload(AlertRule rule) {
        return String.format("{\"reason\":\"%s threshold exceeded\"}", rule.kind().name());
    }
}
