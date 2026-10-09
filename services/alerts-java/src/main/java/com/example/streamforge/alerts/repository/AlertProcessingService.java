package com.example.streamforge.alerts.repository;

import com.example.streamforge.alerts.domain.Alert;
import com.example.streamforge.alerts.domain.AlertRule;
import com.example.streamforge.alerts.domain.RuleEngine;
import com.example.streamforge.alerts.domain.TripEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class AlertProcessingService {

    private final AlertRuleRepository ruleRepository;
    private final AlertRepository alertRepository;
    private final AlertEventOutcomeRepository outcomeRepository;
    private final AlertConsumerFailureRepository failureRepository;
    private final RuleEngine ruleEngine;
    private final ObjectMapper objectMapper;

    public AlertProcessingService(
            AlertRuleRepository ruleRepository,
            AlertRepository alertRepository,
            AlertEventOutcomeRepository outcomeRepository,
            AlertConsumerFailureRepository failureRepository) {
        this.ruleRepository = ruleRepository;
        this.alertRepository = alertRepository;
        this.outcomeRepository = outcomeRepository;
        this.failureRepository = failureRepository;
        this.ruleEngine = new RuleEngine();
        this.objectMapper = new ObjectMapper();
    }

    @Transactional
    public void processEvent(TripEvent event, String consumerGroup, String topic, int partition, long offsetNum) {
        // Idempotency check
        AlertEventOutcomeId outcomeId = new AlertEventOutcomeId(consumerGroup, topic, partition, offsetNum);
        if (outcomeRepository.existsById(outcomeId)) {
            return; // Already processed
        }

        List<AlertRuleEntity> activeRules = ruleRepository.findByEnabledTrue();
        int alertCount = 0;

        for (AlertRuleEntity ruleEntity : activeRules) {
            AlertRule rule = new AlertRule(
                    ruleEntity.getRuleId(),
                    ruleEntity.getRuleVersion(),
                    com.example.streamforge.alerts.domain.RuleKind.valueOf(ruleEntity.getKind()),
                    ruleEntity.getParameters(),
                    ruleEntity.isEnabled()
            );

            Optional<Alert> alertOpt = ruleEngine.evaluate(event, rule);
            if (alertOpt.isPresent()) {
                Alert alert = alertOpt.get();
                AlertEntity entity = new AlertEntity();
                entity.setAlertId(alert.alertId());
                entity.setEventId(alert.eventId());
                entity.setRuleId(alert.ruleId());
                entity.setRuleVersion(alert.ruleVersion());
                entity.setDatasetId(alert.datasetId());
                try {
                    Map<String, Object> payloadMap = objectMapper.readValue(alert.payload(), new TypeReference<>() {});
                    entity.setPayload(payloadMap);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to parse payload", e);
                }
                alertRepository.save(entity);
                alertCount++;
            }
        }

        // Record outcome
        AlertEventOutcomeEntity outcome = new AlertEventOutcomeEntity();
        outcome.setConsumerGroup(consumerGroup);
        outcome.setTopic(topic);
        outcome.setPartition(partition);
        outcome.setOffsetNum(offsetNum);
        outcome.setOutcome(alertCount > 0 ? "ALERT_GENERATED" : "NO_ALERT");
        outcomeRepository.save(outcome);
    }

    @Transactional
    public void recordPermanentFailure(String consumerGroup, String topic, int partition, long offsetNum,
                                       String messageKey, String payloadSha256, String reason) {
        AlertConsumerFailureEntity failure = new AlertConsumerFailureEntity();
        failure.setConsumerGroup(consumerGroup);
        failure.setTopic(topic);
        failure.setPartition(partition);
        failure.setOffsetNum(offsetNum);
        failure.setMessageKey(messageKey);
        failure.setPayloadSha256(payloadSha256);
        failure.setReason(reason);
        failureRepository.save(failure);
    }
}
