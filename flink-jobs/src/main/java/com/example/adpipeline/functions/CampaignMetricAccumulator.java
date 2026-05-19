package com.example.adpipeline.functions;

import java.io.Serializable;
import java.math.BigDecimal;

public class CampaignMetricAccumulator implements Serializable {
    private String campaignId;
    private String advertiserId;
    private long impressions;
    private long clicks;
    private long conversions;
    private BigDecimal cost = BigDecimal.ZERO;
    private BigDecimal revenue = BigDecimal.ZERO;

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

    public void incrementImpressions() {
        this.impressions++;
    }

    public void addImpressions(long value) {
        this.impressions += value;
    }

    public long getClicks() {
        return clicks;
    }

    public void incrementClicks() {
        this.clicks++;
    }

    public void addClicks(long value) {
        this.clicks += value;
    }

    public long getConversions() {
        return conversions;
    }

    public void incrementConversions() {
        this.conversions++;
    }

    public void addConversions(long value) {
        this.conversions += value;
    }

    public BigDecimal getCost() {
        return cost;
    }

    public void addCost(BigDecimal value) {
        this.cost = this.cost.add(value == null ? BigDecimal.ZERO : value);
    }

    public BigDecimal getRevenue() {
        return revenue;
    }

    public void addRevenue(BigDecimal value) {
        this.revenue = this.revenue.add(value == null ? BigDecimal.ZERO : value);
    }
}
