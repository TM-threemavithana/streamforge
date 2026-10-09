package com.example.streamforge.alerts.api;

import com.example.streamforge.alerts.api.dto.AlertDto;
import com.example.streamforge.alerts.api.dto.PagedResponse;
import com.example.streamforge.alerts.api.service.AlertQueryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/alerts-service/alerts")
public class AlertController {

    private final AlertQueryService alertQueryService;

    public AlertController(AlertQueryService alertQueryService) {
        this.alertQueryService = alertQueryService;
    }

    @GetMapping
    public ResponseEntity<PagedResponse<AlertDto>> getAlerts(
            @RequestParam(required = false, name = "dataset_id") String datasetId,
            @RequestParam(required = false, name = "rule_id") String ruleId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(alertQueryService.getAlerts(datasetId, ruleId, page, limit));
    }
}
