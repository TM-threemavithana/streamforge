package com.example.streamforge.alerts.kafka;

import com.example.streamforge.alerts.repository.AlertProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;

@Service
public class AlertsKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertsKafkaConsumer.class);
    private final AlertProcessingService processingService;
    private final ObjectMapper objectMapper;

    public AlertsKafkaConsumer(AlertProcessingService processingService) {
        this.processingService = processingService;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @KafkaListener(topics = "${STREAMFORGE_KAFKA_TOPIC:streamforge.raw-events.v1}")
    public void consume(ConsumerRecord<String, byte[]> record, Acknowledgment ack) {
        try {
            Envelope envelope = objectMapper.readValue(record.value(), Envelope.class);
            
            if (!"streamforge.raw-event:v1".equals(envelope.getSchemaVersion())) {
                handlePermanentFailure(record, "Unsupported schema version: " + envelope.getSchemaVersion());
                ack.acknowledge();
                return;
            }

            if ("TRIP".equals(envelope.getKind())) {
                if (envelope.getTrip() == null) {
                    handlePermanentFailure(record, "Missing trip payload for TRIP kind");
                    ack.acknowledge();
                    return;
                }
                
                // Process event in a transaction
                processingService.processEvent(
                    envelope.getTrip(),
                    "streamforge-alerts-v1",
                    record.topic(),
                    record.partition(),
                    record.offset()
                );
            } else if ("SOURCE_REJECTION".equals(envelope.getKind())) {
                // Ignore rejections for rules engine as defined in ADR-007
                log.debug("Ignoring SOURCE_REJECTION event");
            } else {
                handlePermanentFailure(record, "Unsupported event kind: " + envelope.getKind());
                ack.acknowledge();
                return;
            }

            // Commit offset after successful DB transaction
            ack.acknowledge();

        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            handlePermanentFailure(record, "JSON Parse Error: " + e.getMessage());
            ack.acknowledge(); // Advance past malformed messages
        } catch (Exception e) {
            log.error("Transient error processing Kafka record at {}/{}/{}", 
                    record.topic(), record.partition(), record.offset(), e);
            // DO NOT ack. The framework will sleep and retry if we throw, or crash depending on config.
            // Throwing RuntimeException ensures the transaction (if any) rolls back and offset is not committed.
            throw new RuntimeException("Transient error processing Kafka record", e);
        }
    }

    private void handlePermanentFailure(ConsumerRecord<String, byte[]> record, String reason) {
        String payloadSha256 = hash(record.value());
        processingService.recordPermanentFailure(
                "streamforge-alerts-v1",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                payloadSha256,
                reason
        );
        log.warn("Durably rejected message at {}/{}/{}: {}", record.topic(), record.partition(), record.offset(), reason);
    }

    private String hash(byte[] payload) {
        if (payload == null) return null;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(payload);
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
