package com.example.streamforge.alerts.kafka;

import com.example.streamforge.alerts.domain.RuleKind;
import com.example.streamforge.alerts.repository.AlertEventOutcomeRepository;
import com.example.streamforge.alerts.repository.AlertRepository;
import com.example.streamforge.alerts.repository.AlertRuleEntity;
import com.example.streamforge.alerts.repository.AlertRuleRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "STREAMFORGE_KAFKA_TOPIC=streamforge.raw-events.v1",
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer"
})
@Testcontainers
@EmbeddedKafka(partitions = 1, topics = {"streamforge.raw-events.v1"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
class AlertsKafkaConsumerIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.6-alpine")
            .withDatabaseName("streamforge_alerts")
            .withUsername("alerts")
            .withPassword("secret");

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        // EmbeddedKafka uses a random port; we wire it here so Spring picks it up
        // The actual value is set after context loads via embeddedKafkaBroker, so we use a placeholder
        // and rely on @EmbeddedKafka's spring.embedded.kafka.brokers system property
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers", "localhost:9092"));
    }

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    private AlertRuleRepository ruleRepository;

    @Autowired
    private AlertRepository alertRepository;

    @Autowired
    private AlertEventOutcomeRepository outcomeRepository;

    @BeforeEach
    void setup() {
        if (ruleRepository.count() == 0) {
            AlertRuleEntity rule = new AlertRuleEntity();
            rule.setRuleId("high_fare_test");
            rule.setRuleVersion(1);
            rule.setKind(RuleKind.HIGH_FARE.name());
            rule.setParameters(Map.of("threshold_cents", 5000));
            rule.setEnabled(true);
            ruleRepository.save(rule);
        }
        alertRepository.deleteAll();
        outcomeRepository.deleteAll();
    }

    @Test
    void testConsumerReplayIdempotency() throws Exception {
        String jsonPayload = """
                {
                  "schema_version": "streamforge.raw-event:v1",
                  "kind": "TRIP",
                  "trip": {
                    "datasetId": "ds1",
                    "runId": "run1",
                    "eventId": "evt_duplicate",
                    "sourceSha256": "sha",
                    "sourceRowNumber": 1,
                    "pickupAt": "2024-01-01T10:00:00Z",
                    "dropoffAt": "2024-01-01T10:15:00Z",
                    "pickupZoneId": 1,
                    "distanceMilliMiles": 5000,
                    "fareCents": 10000
                  }
                }
                """;

        // Simulate publishing the same message twice (e.g. producer retry or consumer crash before commit)
        kafkaTemplate.send(new ProducerRecord<>("streamforge.raw-events.v1", "evt_duplicate", jsonPayload.getBytes())).get();
        kafkaTemplate.send(new ProducerRecord<>("streamforge.raw-events.v1", "evt_duplicate", jsonPayload.getBytes())).get();

        // Wait for consumer to process
        TimeUnit.SECONDS.sleep(3);

        // 2 Kafka messages at different offsets → 2 outcomes (each offset is distinct)
        // but only 1 alert because alertId = SHA256(eventId|ruleId|version) is the same
        assertEquals(1, alertRepository.count(), "Alert is content-deduplicated by deterministic alertId");
        assertEquals(2, outcomeRepository.count(), "Each distinct Kafka offset produces one outcome record");
    }
}
