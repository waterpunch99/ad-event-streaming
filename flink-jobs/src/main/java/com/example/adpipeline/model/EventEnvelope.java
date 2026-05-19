package com.example.adpipeline.model;

import java.io.Serializable;

public class EventEnvelope implements Serializable {
    private AdEvent event;
    private String rawJson;

    public EventEnvelope() {
    }

    public EventEnvelope(AdEvent event, String rawJson) {
        this.event = event;
        this.rawJson = rawJson;
    }

    public AdEvent getEvent() {
        return event;
    }

    public void setEvent(AdEvent event) {
        this.event = event;
    }

    public String getRawJson() {
        return rawJson;
    }

    public void setRawJson(String rawJson) {
        this.rawJson = rawJson;
    }

    public String eventId() {
        return event.getEventId();
    }

    public String conversionDedupKey() {
        if ("conversion".equals(event.getEventType()) && event.getConversionId() != null) {
            return "conversion:" + event.getConversionId();
        }
        return "event:" + event.getEventId();
    }
}
