package com.example.streamforge.alerts.api.dto;

import java.time.ZonedDateTime;
import java.util.Map;

public record AlertRuleDto(
        String ruleId,
        Integer ruleVersion,
        String kind,
        Map<String, Object> parameters,
        boolean enabled,
        ZonedDateTime createdAt
) {}
