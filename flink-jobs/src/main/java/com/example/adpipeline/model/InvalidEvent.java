package com.example.adpipeline.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class InvalidEvent implements Serializable {
    private String eventId;
    private String eventType;
    private Instant eventTime;
    private Instant ingestionTime;
    private String userId;
    private String campaignId;
    private String adId;
    private String advertiserId;
    private String rawJson;
    private List<String> errors;

    public InvalidEvent() {
    }

    public InvalidEvent(AdEvent event, String rawJson, List<String> errors) {
        this.eventId = event.getEventId();
        this.eventType = event.getEventType();
        this.eventTime = event.getEventTime();
        this.ingestionTime = event.getIngestionTime();
        this.userId = event.getUserId();
        this.campaignId = event.getCampaignId();
        this.adId = event.getAdId();
        this.advertiserId = event.getAdvertiserId();
        this.rawJson = rawJson;
        this.errors = new ArrayList<>(errors);
    }

    public InvalidEvent(String rawJson, List<String> errors) {
        this.rawJson = rawJson;
        this.errors = new ArrayList<>(errors);
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public Instant getIngestionTime() {
        return ingestionTime;
    }

    public String getUserId() {
        return userId;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public String getAdId() {
        return adId;
    }

    public String getAdvertiserId() {
        return advertiserId;
    }

    public String getRawJson() {
        return rawJson;
    }

    public List<String> getErrors() {
        return errors;
    }
}
