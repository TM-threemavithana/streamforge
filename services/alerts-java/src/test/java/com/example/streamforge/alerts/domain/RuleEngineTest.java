package com.example.streamforge.alerts.domain;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RuleEngineTest {

    private final RuleEngine engine = new RuleEngine();

    @Test
    void testAlertIdDeterministicGeneration() {
        String alertId = Alert.generateAlertId("evt123", "high_fare", 1);
        assertEquals("9b156e785c3c21e87782494b7f9b5ff83c541ab2bf3cad442e194b8d1f1863f3", alertId);
    }

    @Test
    void testHighFareAnomaly() {
        TripEvent event = createEvent(5000L, 15000L, 100L); // $150 fare
        AlertRule rule = new AlertRule("rule1", 1, RuleKind.HIGH_FARE, Map.of("threshold_cents", 10000L), true);
        
        Optional<Alert> alert = engine.evaluate(event, rule);
        assertTrue(alert.isPresent());
        assertEquals("rule1", alert.get().ruleId());
        assertEquals(1, alert.get().ruleVersion());
        assertEquals("{\"reason\":\"HIGH_FARE threshold exceeded\"}", alert.get().payload());
    }

    @Test
    void testHighFareNormal() {
        TripEvent event = createEvent(5000L, 5000L, 100L); // $50 fare
        AlertRule rule = new AlertRule("rule1", 1, RuleKind.HIGH_FARE, Map.of("threshold_cents", 10000L), true);
        
        Optional<Alert> alert = engine.evaluate(event, rule);
        assertFalse(alert.isPresent());
    }

    @Test
    void testLongDistanceAnomaly() {
        TripEvent event = createEvent(60000L, 5000L, 100L); // 60 miles
        AlertRule rule = new AlertRule("rule2", 1, RuleKind.LONG_DISTANCE, Map.of("threshold_milli_miles", 50000L), true);
        
        Optional<Alert> alert = engine.evaluate(event, rule);
        assertTrue(alert.isPresent());
    }

    @Test
    void testUnusualDurationNegative() {
        TripEvent event = createEvent(1000L, 5000L, -100L); // Negative duration
        AlertRule rule = new AlertRule("rule3", 1, RuleKind.UNUSUAL_DURATION, Map.of("max_duration_seconds", 10800L), true);
        
        Optional<Alert> alert = engine.evaluate(event, rule);
        assertTrue(alert.isPresent());
    }

    @Test
    void testUnusualDurationTooLong() {
        TripEvent event = createEvent(1000L, 5000L, 20000L); // 20000s duration (5.5 hrs)
        AlertRule rule = new AlertRule("rule3", 1, RuleKind.UNUSUAL_DURATION, Map.of("max_duration_seconds", 10800L), true);
        
        Optional<Alert> alert = engine.evaluate(event, rule);
        assertTrue(alert.isPresent());
    }
    
    @Test
    void testRuleDisabled() {
        TripEvent event = createEvent(5000L, 15000L, 100L); 
        AlertRule rule = new AlertRule("rule1", 1, RuleKind.HIGH_FARE, Map.of("threshold_cents", 10000L), false); // disabled
        
        Optional<Alert> alert = engine.evaluate(event, rule);
        assertFalse(alert.isPresent());
    }

    private TripEvent createEvent(long distanceMilliMiles, Long fareCents, long durationSeconds) {
        Instant pickup = Instant.parse("2024-01-01T10:00:00Z");
        Instant dropoff = pickup.plusSeconds(durationSeconds);
        return new TripEvent("ds1", "run1", "evt123", "sha", 1L, pickup, dropoff, 1, 2, distanceMilliMiles, fareCents);
    }
}
