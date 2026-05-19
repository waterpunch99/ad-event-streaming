package com.example.adpipeline.serde;

import com.example.adpipeline.model.CampaignMetric;
import com.example.adpipeline.util.JsonUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class CampaignMetricSerializationSchema implements SerializationSchema<CampaignMetric> {
    @Override
    public byte[] serialize(CampaignMetric metric) {
        try {
            return JsonUtils.objectMapper().writeValueAsBytes(metric);
        } catch (Exception exc) {
            throw new IllegalArgumentException("failed to serialize campaign metric", exc);
        }
    }
}
