package com.example.adpipeline.validation;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.adpipeline.model.AdEvent;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventValidatorTest {
    private final EventValidator validator = new EventValidator();

    @Test
    void validImpressionPasses() {
        ValidationResult result = validator.validate(validEvent("impression"));

        assertTrue(result.valid());
        assertTrue(result.errors().isEmpty());
    }

    @Test
    void invalidEventTypeFails() {
        AdEvent event = validEvent("view");

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());
        assertTrue(result.errors().contains("event_type must be one of: impression, click, conversion"));
    }

    @Test
    void requiredFieldsCannotBeBlank() {
        AdEvent event = validEvent("click");
        event.setEventId("");

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());
        assertTrue(result.errors().contains("event_id is required"));
    }

    @Test
    void negativeCostFails() {
        AdEvent event = validEvent("click");
        event.setCost(new BigDecimal("-0.01"));

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());
        assertTrue(result.errors().contains("cost must be greater than or equal to 0"));
    }

    @Test
    void conversionRequiresConversionId() {
        AdEvent event = validEvent("conversion");
        event.setConversionId(null);

        ValidationResult result = validator.validate(event);

        assertFalse(result.valid());
        assertTrue(result.errors().contains("conversion_id is required for conversion events"));
    }

    private static AdEvent validEvent(String eventType) {
        AdEvent event = new AdEvent();
        event.setEventId("evt-001");
        event.setEventType(eventType);
        event.setEventTime(Instant.parse("2026-05-19T08:00:00Z"));
        event.setIngestionTime(Instant.parse("2026-05-19T08:00:01Z"));
        event.setUserId("user-001");
        event.setSessionId("session-001");
        event.setCampaignId("campaign-001");
        event.setAdId("ad-001");
        event.setAdvertiserId("advertiser-001");
        event.setCategory("sports");
        event.setDeviceType("mobile");
        event.setOs("ios");
        event.setCountry("KR");
        event.setPlacement("feed");
        event.setCost(new BigDecimal("1.000000"));
        event.setRevenue(BigDecimal.ZERO);
        if ("conversion".equals(eventType)) {
            event.setConversionId("conv-001");
            event.setConversionValue(new BigDecimal("25.000000"));
            event.setAttributedClickId("click-001");
        }
        return event;
    }
}
