package com.example.streamforge.alerts.api.service;

import com.example.streamforge.alerts.api.dto.AlertDto;
import com.example.streamforge.alerts.api.dto.PagedResponse;
import com.example.streamforge.alerts.repository.AlertEntity;
import com.example.streamforge.alerts.repository.AlertRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class AlertQueryService {

    private final AlertRepository alertRepository;

    public AlertQueryService(AlertRepository alertRepository) {
        this.alertRepository = alertRepository;
    }

    @Transactional(readOnly = true)
    public PagedResponse<AlertDto> getAlerts(String datasetId, String ruleId, int page, int limit) {
        int boundedPage = Math.max(0, page);
        int boundedLimit = Math.max(1, Math.min(500, limit));

        Pageable pageable = PageRequest.of(boundedPage, boundedLimit, Sort.by(Sort.Direction.DESC, "createdAt"));

        Page<AlertEntity> resultPage;
        boolean hasDataset = datasetId != null && !datasetId.isBlank();
        boolean hasRule = ruleId != null && !ruleId.isBlank();

        if (hasDataset && hasRule) {
            resultPage = alertRepository.findByDatasetIdAndRuleId(datasetId, ruleId, pageable);
        } else if (hasDataset) {
            resultPage = alertRepository.findByDatasetId(datasetId, pageable);
        } else if (hasRule) {
            resultPage = alertRepository.findByRuleId(ruleId, pageable);
        } else {
            resultPage = alertRepository.findAll(pageable);
        }

        List<AlertDto> dtos = resultPage.getContent().stream()
                .map(this::toDto)
                .toList();

        return new PagedResponse<>(dtos, boundedPage, boundedLimit, resultPage.getTotalElements());
    }

    private AlertDto toDto(AlertEntity entity) {
        return new AlertDto(
                entity.getAlertId(),
                entity.getEventId(),
                entity.getRuleId(),
                entity.getRuleVersion(),
                entity.getDatasetId(),
                entity.getPayload(),
                entity.getCreatedAt()
        );
    }
}
