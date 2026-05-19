package com.example.adpipeline.validation;

import com.example.adpipeline.model.AdEvent;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class EventValidator {
    private static final Set<String> VALID_EVENT_TYPES = Set.of("impression", "click", "conversion");

    public ValidationResult validate(AdEvent event) {
        List<String> errors = new ArrayList<>();

        if (event == null) {
            return ValidationResult.invalid(List.of("event is required"));
        }

        requireText(event.getEventId(), "event_id", errors);
        requireText(event.getEventType(), "event_type", errors);
        requirePresent(event.getEventTime(), "event_time", errors);
        requirePresent(event.getIngestionTime(), "ingestion_time", errors);
        requireText(event.getUserId(), "user_id", errors);
        requireText(event.getCampaignId(), "campaign_id", errors);
        requireText(event.getAdId(), "ad_id", errors);
        requireText(event.getAdvertiserId(), "advertiser_id", errors);

        if (hasText(event.getEventType()) && !VALID_EVENT_TYPES.contains(event.getEventType())) {
            errors.add("event_type must be one of: impression, click, conversion");
        }

        validateNonNegative(event.getCost(), "cost", errors);
        validateNonNegative(event.getRevenue(), "revenue", errors);

        if ("conversion".equals(event.getEventType()) && !hasText(event.getConversionId())) {
            errors.add("conversion_id is required for conversion events");
        }

        if (event.getConversionValue() != null) {
            validateNonNegative(event.getConversionValue(), "conversion_value", errors);
        }

        if (errors.isEmpty()) {
            return ValidationResult.ok();
        }
        return ValidationResult.invalid(errors);
    }

    private static void requireText(String value, String fieldName, List<String> errors) {
        if (!hasText(value)) {
            errors.add(fieldName + " is required");
        }
    }

    private static void requirePresent(Object value, String fieldName, List<String> errors) {
        if (value == null) {
            errors.add(fieldName + " is required");
        }
    }

    private static void validateNonNegative(BigDecimal value, String fieldName, List<String> errors) {
        if (value == null) {
            errors.add(fieldName + " is required");
            return;
        }
        if (value.signum() < 0) {
            errors.add(fieldName + " must be greater than or equal to 0");
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
