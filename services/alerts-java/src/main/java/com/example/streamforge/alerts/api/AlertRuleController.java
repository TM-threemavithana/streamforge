package com.example.streamforge.alerts.api;

import com.example.streamforge.alerts.api.dto.AlertRuleDto;
import com.example.streamforge.alerts.api.dto.CreateRuleRequest;
import com.example.streamforge.alerts.api.dto.UpdateRuleStatusRequest;
import com.example.streamforge.alerts.api.service.RuleManagementService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/alerts-service/rules")
public class AlertRuleController {

    private final RuleManagementService ruleService;

    public AlertRuleController(RuleManagementService ruleService) {
        this.ruleService = ruleService;
    }

    @GetMapping
    public ResponseEntity<List<AlertRuleDto>> getAllRules() {
        return ResponseEntity.ok(ruleService.getAllRules());
    }

    @GetMapping("/{ruleId}")
    public ResponseEntity<AlertRuleDto> getRule(
            @PathVariable String ruleId,
            @RequestParam(required = false) Integer version) {
        return ResponseEntity.ok(ruleService.getRule(ruleId, version));
    }

    @PostMapping
    public ResponseEntity<AlertRuleDto> createRule(@Valid @RequestBody CreateRuleRequest request) {
        AlertRuleDto created = ruleService.createRule(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @PatchMapping("/{ruleId}/status")
    public ResponseEntity<AlertRuleDto> updateRuleStatus(
            @PathVariable String ruleId,
            @RequestParam(required = false) Integer version,
            @Valid @RequestBody UpdateRuleStatusRequest request) {
        Integer targetVersion = version != null ? version : request.ruleVersion();
        return ResponseEntity.ok(ruleService.updateRuleStatus(ruleId, targetVersion, request.enabled()));
    }

    @PatchMapping("/{ruleId}/versions/{ruleVersion}/status")
    public ResponseEntity<AlertRuleDto> updateRuleVersionStatus(
            @PathVariable String ruleId,
            @PathVariable Integer ruleVersion,
            @Valid @RequestBody UpdateRuleStatusRequest request) {
        return ResponseEntity.ok(ruleService.updateRuleStatus(ruleId, ruleVersion, request.enabled()));
    }
}
