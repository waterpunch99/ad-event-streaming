package com.example.adpipeline.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.example.adpipeline.model.AdEvent;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class CampaignMetricAggregatorTest {
    private final CampaignMetricAggregator aggregator = new CampaignMetricAggregator();

    @Test
    void countsEventsAndSumsMoney() {
        CampaignMetricAccumulator accumulator = aggregator.createAccumulator();

        aggregator.add(event("impression", "0.010000", "0.000000"), accumulator);
        aggregator.add(event("click", "1.250000", "0.000000"), accumulator);
        aggregator.add(event("conversion", "2.000000", "10.000000"), accumulator);

        assertEquals(1, accumulator.getImpressions());
        assertEquals(1, accumulator.getClicks());
        assertEquals(1, accumulator.getConversions());
        assertEquals(new BigDecimal("3.260000"), accumulator.getCost());
        assertEquals(new BigDecimal("10.000000"), accumulator.getRevenue());
    }

    @Test
    void safeDivideReturnsZeroForZeroDenominator() {
        assertEquals(0.0d, CampaignMetricAggregator.safeDivide(10, 0));
        assertEquals(0.0d, CampaignMetricAggregator.safeDivide(new BigDecimal("10.0"), BigDecimal.ZERO));
    }

    @Test
    void safeDivideCalculatesRates() {
        assertEquals(0.5d, CampaignMetricAggregator.safeDivide(5, 10));
        assertEquals(2.5d, CampaignMetricAggregator.safeDivide(new BigDecimal("25.0"), new BigDecimal("10.0")));
    }

    private static AdEvent event(String eventType, String cost, String revenue) {
        AdEvent event = new AdEvent();
        event.setEventType(eventType);
        event.setCampaignId("campaign-001");
        event.setAdvertiserId("advertiser-001");
        event.setCost(new BigDecimal(cost));
        event.setRevenue(new BigDecimal(revenue));
        return event;
    }
}
