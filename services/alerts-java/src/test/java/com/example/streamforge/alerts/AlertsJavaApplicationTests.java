package com.example.streamforge.alerts;

import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Smoke test: verifies the full Spring context loads correctly with a real DB.
 * Kafka auto-configuration is excluded because there's no broker needed for context loading.
 */
@SpringBootTest(properties = {
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration,org.springframework.kafka.annotation.EnableKafkaConfigurationSelector"
})
@Testcontainers
@ActiveProfiles("test")
class AlertsJavaApplicationTests {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17.6-alpine")
            .withDatabaseName("streamforge_alerts")
            .withUsername("alerts")
            .withPassword("secret");

    @Test
    void contextLoads() {
        // Spring context starts successfully with a real Postgres via Testcontainers
    }
}
