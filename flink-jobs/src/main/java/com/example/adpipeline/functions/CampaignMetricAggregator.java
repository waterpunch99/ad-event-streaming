package com.example.adpipeline.functions;

import com.example.adpipeline.model.AdEvent;
import java.math.BigDecimal;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CampaignMetricAggregator implements AggregateFunction<AdEvent, CampaignMetricAccumulator, CampaignMetricAccumulator> {
    @Override
    public CampaignMetricAccumulator createAccumulator() {
        return new CampaignMetricAccumulator();
    }

    @Override
    public CampaignMetricAccumulator add(AdEvent event, CampaignMetricAccumulator accumulator) {
        accumulator.setCampaignId(event.getCampaignId());
        accumulator.setAdvertiserId(event.getAdvertiserId());

        if ("impression".equals(event.getEventType())) {
            accumulator.incrementImpressions();
        } else if ("click".equals(event.getEventType())) {
            accumulator.incrementClicks();
        } else if ("conversion".equals(event.getEventType())) {
            accumulator.incrementConversions();
        }

        accumulator.addCost(event.getCost());
        accumulator.addRevenue(event.getRevenue());
        return accumulator;
    }

    @Override
    public CampaignMetricAccumulator getResult(CampaignMetricAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public CampaignMetricAccumulator merge(CampaignMetricAccumulator left, CampaignMetricAccumulator right) {
        CampaignMetricAccumulator merged = new CampaignMetricAccumulator();
        merged.setCampaignId(left.getCampaignId() != null ? left.getCampaignId() : right.getCampaignId());
        merged.setAdvertiserId(left.getAdvertiserId() != null ? left.getAdvertiserId() : right.getAdvertiserId());
        addCounts(merged, left);
        addCounts(merged, right);
        return merged;
    }

    public static double safeDivide(long numerator, long denominator) {
        if (denominator == 0) {
            return 0.0d;
        }
        return (double) numerator / (double) denominator;
    }

    public static double safeDivide(BigDecimal numerator, BigDecimal denominator) {
        if (denominator == null || BigDecimal.ZERO.compareTo(denominator) == 0) {
            return 0.0d;
        }
        BigDecimal safeNumerator = numerator == null ? BigDecimal.ZERO : numerator;
        return safeNumerator.doubleValue() / denominator.doubleValue();
    }

    private static void addCounts(CampaignMetricAccumulator target, CampaignMetricAccumulator source) {
        target.addImpressions(source.getImpressions());
        target.addClicks(source.getClicks());
        target.addConversions(source.getConversions());
        target.addCost(source.getCost());
        target.addRevenue(source.getRevenue());
    }
}
