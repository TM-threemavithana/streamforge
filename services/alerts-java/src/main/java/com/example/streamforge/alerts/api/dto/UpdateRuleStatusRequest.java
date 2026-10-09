package com.example.streamforge.alerts.api.dto;

import jakarta.validation.constraints.NotNull;

public record UpdateRuleStatusRequest(
        @NotNull(message = "enabled is required")
        Boolean enabled,

        Integer ruleVersion
) {}
