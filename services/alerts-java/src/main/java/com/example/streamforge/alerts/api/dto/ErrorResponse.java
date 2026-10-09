package com.example.streamforge.alerts.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ErrorResponse(
        String code,
        String message,
        String requestId,
        Object details
) {
    public ErrorResponse(String code, String message) {
        this(code, message, null, null);
    }
}
