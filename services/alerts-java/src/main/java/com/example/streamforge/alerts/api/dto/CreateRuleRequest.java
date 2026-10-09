package com.example.streamforge.alerts.api.dto;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public record CreateRuleRequest(
        @NotBlank(message = "ruleId is required")
        String ruleId,

        Integer ruleVersion,

        @NotBlank(message = "kind is required")
        String kind,

        Map<String, Object> parameters,

        Boolean enabled
) {}
