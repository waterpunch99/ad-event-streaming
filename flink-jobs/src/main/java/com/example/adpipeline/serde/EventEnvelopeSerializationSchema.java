package com.example.adpipeline.serde;

import com.example.adpipeline.model.EventEnvelope;
import com.example.adpipeline.util.JsonUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class EventEnvelopeSerializationSchema implements SerializationSchema<EventEnvelope> {
    @Override
    public byte[] serialize(EventEnvelope envelope) {
        try {
            return JsonUtils.objectMapper().writeValueAsBytes(envelope.getEvent());
        } catch (Exception exc) {
            throw new IllegalArgumentException("failed to serialize clean ad event", exc);
        }
    }
}
