package com.example.streamforge.alerts.api.service;

import com.example.streamforge.alerts.api.dto.AlertRuleDto;
import com.example.streamforge.alerts.api.dto.CreateRuleRequest;
import com.example.streamforge.alerts.api.exception.ConflictException;
import com.example.streamforge.alerts.api.exception.ResourceNotFoundException;
import com.example.streamforge.alerts.api.exception.ValidationException;
import com.example.streamforge.alerts.domain.RuleKind;
import com.example.streamforge.alerts.repository.AlertRuleEntity;
import com.example.streamforge.alerts.repository.AlertRuleRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class RuleManagementService {

    private final AlertRuleRepository ruleRepository;

    public RuleManagementService(AlertRuleRepository ruleRepository) {
        this.ruleRepository = ruleRepository;
    }

    @Transactional(readOnly = true)
    public List<AlertRuleDto> getAllRules() {
        return ruleRepository.findAllByOrderByRuleIdAscRuleVersionDesc()
                .stream()
                .map(this::toDto)
                .toList();
    }

    @Transactional(readOnly = true)
    public AlertRuleDto getRule(String ruleId, Integer version) {
        AlertRuleEntity entity;
        if (version != null) {
            entity = ruleRepository.findByRuleIdAndRuleVersion(ruleId, version)
                    .orElseThrow(() -> new ResourceNotFoundException("Rule not found: " + ruleId + " version " + version));
        } else {
            entity = ruleRepository.findTopByRuleIdOrderByRuleVersionDesc(ruleId)
                    .orElseThrow(() -> new ResourceNotFoundException("Rule not found: " + ruleId));
        }
        return toDto(entity);
    }

    @Transactional
    public AlertRuleDto createRule(CreateRuleRequest request) {
        RuleKind kind;
        try {
            kind = RuleKind.valueOf(request.kind().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ValidationException("Invalid rule kind: " + request.kind() + ". Must be one of HIGH_FARE, LONG_DISTANCE, UNUSUAL_DURATION");
        }

        Map<String, Object> params = request.parameters() != null ? request.parameters() : Collections.emptyMap();
        validateParameters(kind, params);

        int version;
        if (request.ruleVersion() != null) {
            version = request.ruleVersion();
            if (ruleRepository.existsByRuleIdAndRuleVersion(request.ruleId(), version)) {
                throw new ConflictException("Rule '" + request.ruleId() + "' version " + version + " already exists");
            }
        } else {
            Optional<AlertRuleEntity> latest = ruleRepository.findTopByRuleIdOrderByRuleVersionDesc(request.ruleId());
            version = latest.map(alertRuleEntity -> alertRuleEntity.getRuleVersion() + 1).orElse(1);
        }

        AlertRuleEntity entity = new AlertRuleEntity();
        entity.setRuleId(request.ruleId());
        entity.setRuleVersion(version);
        entity.setKind(kind.name());
        entity.setParameters(params);
        entity.setEnabled(request.enabled() == null || request.enabled());

        AlertRuleEntity saved = ruleRepository.save(entity);
        return toDto(saved);
    }

    @Transactional
    public AlertRuleDto updateRuleStatus(String ruleId, Integer version, boolean enabled) {
        AlertRuleEntity entity;
        if (version != null) {
            entity = ruleRepository.findByRuleIdAndRuleVersion(ruleId, version)
                    .orElseThrow(() -> new ResourceNotFoundException("Rule not found: " + ruleId + " version " + version));
        } else {
            entity = ruleRepository.findTopByRuleIdOrderByRuleVersionDesc(ruleId)
                    .orElseThrow(() -> new ResourceNotFoundException("Rule not found: " + ruleId));
        }

        entity.setEnabled(enabled);
        AlertRuleEntity saved = ruleRepository.save(entity);
        return toDto(saved);
    }

    private void validateParameters(RuleKind kind, Map<String, Object> params) {
        switch (kind) {
            case HIGH_FARE -> {
                if (params.containsKey("threshold_cents") && !(params.get("threshold_cents") instanceof Number)) {
                    throw new ValidationException("threshold_cents must be a numeric value");
                }
            }
            case LONG_DISTANCE -> {
                if (params.containsKey("threshold_milli_miles") && !(params.get("threshold_milli_miles") instanceof Number)) {
                    throw new ValidationException("threshold_milli_miles must be a numeric value");
                }
            }
            case UNUSUAL_DURATION -> {
                if (params.containsKey("max_duration_seconds") && !(params.get("max_duration_seconds") instanceof Number)) {
                    throw new ValidationException("max_duration_seconds must be a numeric value");
                }
            }
        }
    }

    private AlertRuleDto toDto(AlertRuleEntity entity) {
        return new AlertRuleDto(
                entity.getRuleId(),
                entity.getRuleVersion(),
                entity.getKind(),
                entity.getParameters(),
                entity.isEnabled(),
                entity.getCreatedAt()
        );
    }
}
