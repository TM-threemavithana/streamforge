package com.example.streamforge.alerts.api.dto;

import java.util.List;

public record PagedResponse<T>(
        List<T> items,
        int page,
        int limit,
        long total
) {}
