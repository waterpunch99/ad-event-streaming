package com.example.adpipeline.functions;

import com.example.adpipeline.model.CampaignMetric;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CampaignMetricWindowFunction
    extends ProcessWindowFunction<CampaignMetricAccumulator, CampaignMetric, String, TimeWindow> {

    @Override
    public void process(
        String key,
        Context context,
        Iterable<CampaignMetricAccumulator> elements,
        Collector<CampaignMetric> out
    ) {
        CampaignMetricAccumulator accumulator = elements.iterator().next();
        CampaignMetric metric = new CampaignMetric();
        metric.setWindowStart(Instant.ofEpochMilli(context.window().getStart()));
        metric.setWindowEnd(Instant.ofEpochMilli(context.window().getEnd()));
        metric.setCampaignId(accumulator.getCampaignId());
        metric.setAdvertiserId(accumulator.getAdvertiserId());
        metric.setImpressions(accumulator.getImpressions());
        metric.setClicks(accumulator.getClicks());
        metric.setConversions(accumulator.getConversions());
        metric.setCtr(CampaignMetricAggregator.safeDivide(accumulator.getClicks(), accumulator.getImpressions()));
        metric.setCvr(CampaignMetricAggregator.safeDivide(accumulator.getConversions(), accumulator.getClicks()));
        metric.setCost(accumulator.getCost());
        metric.setRevenue(accumulator.getRevenue());
        metric.setRoas(CampaignMetricAggregator.safeDivide(accumulator.getRevenue(), accumulator.getCost()));
        out.collect(metric);
    }
}
