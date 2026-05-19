package com.example.adpipeline.model;

import java.io.Serializable;
import java.time.Instant;

public class LateEvent implements Serializable {
    private EventEnvelope envelope;
    private Instant watermarkTime;

    public LateEvent() {
    }

    public LateEvent(EventEnvelope envelope, Instant watermarkTime) {
        this.envelope = envelope;
        this.watermarkTime = watermarkTime;
    }

    public EventEnvelope getEnvelope() {
        return envelope;
    }

    public Instant getWatermarkTime() {
        return watermarkTime;
    }
}
