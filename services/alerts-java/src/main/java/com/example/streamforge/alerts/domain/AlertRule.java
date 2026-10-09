package com.example.streamforge.alerts.domain;

import java.util.Map;

public record AlertRule(
        String ruleId,
        int ruleVersion,
        RuleKind kind,
        Map<String, Object> parameters,
        boolean enabled
) {}
