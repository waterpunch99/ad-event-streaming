package com.example.adpipeline.model;

import java.math.BigDecimal;
import java.time.Instant;

public class CampaignMetric {
    private Instant windowStart;
    private Instant windowEnd;
    private String campaignId;
    private String advertiserId;
    private long impressions;
    private long clicks;
    private long conversions;
    private double ctr;
    private double cvr;
    private BigDecimal cost = BigDecimal.ZERO;
    private BigDecimal revenue = BigDecimal.ZERO;
    private double roas;

    public Instant getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Instant windowStart) {
        this.windowStart = windowStart;
    }

    public Instant getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Instant windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(String campaignId) {
        this.campaignId = campaignId;
    }

    public String getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(String advertiserId) {
        this.advertiserId = advertiserId;
    }

    public long getImpressions() {
        return impressions;
    }

    public void setImpressions(long impressions) {
        this.impressions = impressions;
    }

    public long getClicks() {
        return clicks;
    }

    public void setClicks(long clicks) {
        this.clicks = clicks;
    }

    public long getConversions() {
        return conversions;
    }

    public void setConversions(long conversions) {
        this.conversions = conversions;
    }

    public double getCtr() {
        return ctr;
    }

    public void setCtr(double ctr) {
        this.ctr = ctr;
    }

    public double getCvr() {
        return cvr;
    }

    public void setCvr(double cvr) {
        this.cvr = cvr;
    }

    public BigDecimal getCost() {
        return cost;
    }

    public void setCost(BigDecimal cost) {
        this.cost = cost;
    }

    public BigDecimal getRevenue() {
        return revenue;
    }

    public void setRevenue(BigDecimal revenue) {
        this.revenue = revenue;
    }

    public double getRoas() {
        return roas;
    }

    public void setRoas(double roas) {
        this.roas = roas;
    }
}
