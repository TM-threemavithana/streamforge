package com.example.streamforge.alerts.domain;

import java.time.Instant;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

public record Alert(
        String alertId,
        String eventId,
        String ruleId,
        int ruleVersion,
        String datasetId,
        String payload,
        Instant createdAt
) {
    public static String generateAlertId(String eventId, String ruleId, int ruleVersion) {
        try {
            String input = eventId + "|" + ruleId + "|" + ruleVersion;
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate alert ID", e);
        }
    }
}
