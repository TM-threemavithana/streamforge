package com.example.streamforge.alerts.api.dto;

import java.time.ZonedDateTime;
import java.util.Map;

public record AlertDto(
        String alertId,
        String eventId,
        String ruleId,
        Integer ruleVersion,
        String datasetId,
        Map<String, Object> payload,
        ZonedDateTime createdAt
) {}
