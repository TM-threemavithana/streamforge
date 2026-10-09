package com.example.streamforge.alerts.api;

import com.example.streamforge.alerts.domain.RuleKind;
import com.example.streamforge.alerts.repository.AlertEntity;
import com.example.streamforge.alerts.repository.AlertRepository;
import com.example.streamforge.alerts.repository.AlertRuleEntity;
import com.example.streamforge.alerts.repository.AlertRuleRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
@ActiveProfiles("test")
class AlertApiControllerIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.6-alpine")
            .withDatabaseName("streamforge_alerts")
            .withUsername("alerts")
            .withPassword("secret");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private AlertRuleRepository ruleRepository;

    @Autowired
    private AlertRepository alertRepository;

    @BeforeEach
    void setup() {
        alertRepository.deleteAll();
        ruleRepository.deleteAll();
    }

    @Test
    void testHealthEndpoints() throws Exception {
        mockMvc.perform(get("/health/live"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("live"));

        mockMvc.perform(get("/health/ready"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ready"));
    }

    @Test
    void testRulesLifecycle() throws Exception {
        // 1. Initially empty
        mockMvc.perform(get("/api/v1/alerts-service/rules"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));

        // 2. Create rule
        String createJson = """
                {
                    "ruleId": "rule_high_fare",
                    "kind": "HIGH_FARE",
                    "parameters": {"threshold_cents": 15000},
                    "enabled": true
                }
                """;

        mockMvc.perform(post("/api/v1/alerts-service/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(createJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.ruleId").value("rule_high_fare"))
                .andExpect(jsonPath("$.ruleVersion").value(1))
                .andExpect(jsonPath("$.kind").value("HIGH_FARE"))
                .andExpect(jsonPath("$.enabled").value(true));

        // 3. Duplicate creation with same version -> 409 Conflict
        String dupJson = """
                {
                    "ruleId": "rule_high_fare",
                    "ruleVersion": 1,
                    "kind": "HIGH_FARE",
                    "parameters": {"threshold_cents": 20000}
                }
                """;

        mockMvc.perform(post("/api/v1/alerts-service/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(dupJson))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("CONFLICT"));

        // 4. Invalid kind -> 400 Bad Request
        String invalidKindJson = """
                {
                    "ruleId": "rule_invalid",
                    "kind": "UNKNOWN_KIND"
                }
                """;

        mockMvc.perform(post("/api/v1/alerts-service/rules")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidKindJson))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"));

        // 5. Disable rule via PATCH
        String patchJson = """
                {
                    "enabled": false
                }
                """;

        mockMvc.perform(patch("/api/v1/alerts-service/rules/rule_high_fare/status")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(patchJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.enabled").value(false));

        // 6. Get single rule
        mockMvc.perform(get("/api/v1/alerts-service/rules/rule_high_fare"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ruleId").value("rule_high_fare"))
                .andExpect(jsonPath("$.enabled").value(false));
    }

    @Test
    void testAlertsPaginationAndFiltering() throws Exception {
        // Setup a rule
        AlertRuleEntity rule = new AlertRuleEntity();
        rule.setRuleId("test_rule");
        rule.setRuleVersion(1);
        rule.setKind(RuleKind.HIGH_FARE.name());
        rule.setParameters(Map.of("threshold_cents", 10000));
        rule.setEnabled(true);
        ruleRepository.save(rule);

        // Seed 3 alerts: 2 for ds1, 1 for ds2
        AlertEntity a1 = new AlertEntity();
        a1.setAlertId("alert_1");
        a1.setEventId("evt_1");
        a1.setRuleId("test_rule");
        a1.setRuleVersion(1);
        a1.setDatasetId("ds1");
        a1.setPayload(Map.of("fare", 12000));
        alertRepository.save(a1);

        AlertEntity a2 = new AlertEntity();
        a2.setAlertId("alert_2");
        a2.setEventId("evt_2");
        a2.setRuleId("test_rule");
        a2.setRuleVersion(1);
        a2.setDatasetId("ds1");
        a2.setPayload(Map.of("fare", 15000));
        alertRepository.save(a2);

        AlertEntity a3 = new AlertEntity();
        a3.setAlertId("alert_3");
        a3.setEventId("evt_3");
        a3.setRuleId("test_rule");
        a3.setRuleVersion(1);
        a3.setDatasetId("ds2");
        a3.setPayload(Map.of("fare", 20000));
        alertRepository.save(a3);

        // Query all
        mockMvc.perform(get("/api/v1/alerts-service/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(3))
                .andExpect(jsonPath("$.items", hasSize(3)));

        // Filter by dataset_id=ds1
        mockMvc.perform(get("/api/v1/alerts-service/alerts?dataset_id=ds1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(2))
                .andExpect(jsonPath("$.items", hasSize(2)));

        // Filter by dataset_id=ds2
        mockMvc.perform(get("/api/v1/alerts-service/alerts?dataset_id=ds2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].alertId").value("alert_3"));

        // Pagination: limit=1
        mockMvc.perform(get("/api/v1/alerts-service/alerts?limit=1&page=0"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(3))
                .andExpect(jsonPath("$.items", hasSize(1)));
    }
}
