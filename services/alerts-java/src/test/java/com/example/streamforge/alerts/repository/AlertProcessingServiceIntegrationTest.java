package com.example.streamforge.alerts.repository;

import com.example.streamforge.alerts.domain.RuleKind;
import com.example.streamforge.alerts.domain.TripEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class AlertProcessingServiceIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.6-alpine")
            .withDatabaseName("streamforge_alerts")
            .withUsername("alerts")
            .withPassword("secret");

    @Autowired
    private AlertProcessingService alertProcessingService;

    @Autowired
    private AlertRuleRepository ruleRepository;

    @Autowired
    private AlertRepository alertRepository;

    @Autowired
    private AlertEventOutcomeRepository outcomeRepository;

    @Test
    void testProcessEventWithAlertGeneration() {
        // Setup Rule
        AlertRuleEntity rule = new AlertRuleEntity();
        rule.setRuleId("high_fare_test");
        rule.setRuleVersion(1);
        rule.setKind(RuleKind.HIGH_FARE.name());
        rule.setParameters(Map.of("threshold_cents", 5000)); // $50 threshold
        rule.setEnabled(true);
        ruleRepository.save(rule);

        // Process anomalous event
        TripEvent event = new TripEvent(
                "ds1", "run1", "evt_high", "sha1", 100,
                Instant.parse("2024-01-01T10:00:00Z"), Instant.parse("2024-01-01T10:15:00Z"),
                1, 2, 5000L, 8000L // 8000 cents > 5000 cents
        );

        alertProcessingService.processEvent(event, "group1", "topic1", 0, 1L);

        // Verify alert and outcome are saved atomically
        assertEquals(1, alertRepository.count());
        assertTrue(alertRepository.findAll().get(0).getEventId().equals("evt_high"));
        
        assertEquals(1, outcomeRepository.count());
        assertEquals("ALERT_GENERATED", outcomeRepository.findAll().get(0).getOutcome());

        // Test Idempotency
        alertProcessingService.processEvent(event, "group1", "topic1", 0, 1L);
        assertEquals(1, outcomeRepository.count()); // Still 1 outcome
        assertEquals(1, alertRepository.count()); // Still 1 alert
    }
}
