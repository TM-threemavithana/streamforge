package com.example.streamforge.alerts.domain;

import java.time.Instant;

public record TripEvent(
        String datasetId,
        String runId,
        String eventId,
        String sourceSha256,
        long sourceRowNumber,
        Instant pickupAt,
        Instant dropoffAt,
        int pickupZoneId,
        Integer dropoffZoneId,
        long distanceMilliMiles,
        Long fareCents
) {}
