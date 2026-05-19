package com.example.adpipeline.serde;

import com.example.adpipeline.model.EventEnvelope;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class EventEnvelopeKeySerializationSchema implements SerializationSchema<EventEnvelope> {
    @Override
    public byte[] serialize(EventEnvelope envelope) {
        return envelope.getEvent().getUserId().getBytes(StandardCharsets.UTF_8);
    }
}
