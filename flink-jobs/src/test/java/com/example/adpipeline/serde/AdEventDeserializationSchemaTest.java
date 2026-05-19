package com.example.adpipeline.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.example.adpipeline.model.AdEvent;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AdEventDeserializationSchemaTest {
    @Test
    void deserializesSnakeCaseJson() throws Exception {
        String json = """
            {
              "event_id": "evt-001",
              "event_type": "click",
              "event_time": "2026-05-19T08:00:00Z",
              "ingestion_time": "2026-05-19T08:00:01Z",
              "user_id": "user-001",
              "session_id": "session-001",
              "campaign_id": "campaign-001",
              "ad_id": "ad-001",
              "advertiser_id": "advertiser-001",
              "category": "sports",
              "device_type": "mobile",
              "os": "ios",
              "country": "KR",
              "placement": "feed",
              "cost": "1.250000",
              "revenue": "0.000000"
            }
            """;

        AdEvent event = new AdEventDeserializationSchema().deserialize(json.getBytes(StandardCharsets.UTF_8));

        assertEquals("evt-001", event.getEventId());
        assertEquals("click", event.getEventType());
        assertEquals("campaign-001", event.getCampaignId());
        assertEquals(new BigDecimal("1.250000"), event.getCost());
    }
}
