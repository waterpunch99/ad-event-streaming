package com.example.adpipeline.sink;

import com.example.adpipeline.model.AdEvent;
import com.example.adpipeline.model.DuplicateEvent;
import com.example.adpipeline.model.EventEnvelope;
import com.example.adpipeline.model.InvalidEvent;
import com.example.adpipeline.model.LateEvent;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.StringJoiner;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public final class PostgresSinkFactory {
    private static final JdbcExecutionOptions EXECUTION_OPTIONS = JdbcExecutionOptions.builder()
        .withBatchSize(500)
        .withBatchIntervalMs(1000)
        .withMaxRetries(3)
        .build();

    private PostgresSinkFactory() {
    }

    public static SinkFunction<EventEnvelope> silverAdEvents(String jdbcUrl, String username, String password) {
        String sql = """
            INSERT INTO silver_ad_events (
                event_id, event_type, event_time, ingestion_time, user_id, session_id,
                campaign_id, ad_id, advertiser_id, category, device_type, os, country,
                placement, cost, revenue, conversion_id, conversion_value,
                attributed_click_id, raw_payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (event_id) DO NOTHING
            """;
        return JdbcSink.sink(sql, (statement, envelope) -> bindAdEvent(statement, envelope), EXECUTION_OPTIONS,
            connectionOptions(jdbcUrl, username, password));
    }

    public static SinkFunction<InvalidEvent> silverInvalidEvents(String jdbcUrl, String username, String password) {
        String sql = """
            INSERT INTO silver_invalid_events (
                event_id, event_type, event_time, ingestion_time, user_id, campaign_id,
                ad_id, advertiser_id, raw_payload, validation_errors
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, string_to_array(?, '|'))
            """;
        return JdbcSink.sink(sql, PostgresSinkFactory::bindInvalidEvent, EXECUTION_OPTIONS,
            connectionOptions(jdbcUrl, username, password));
    }

    public static SinkFunction<DuplicateEvent> silverDuplicateEvents(String jdbcUrl, String username, String password) {
        String sql = """
            INSERT INTO silver_duplicate_events (
                event_id, event_type, event_time, ingestion_time, user_id, campaign_id,
                ad_id, advertiser_id, conversion_id, duplicate_key_type,
                duplicate_key_value, raw_payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            """;
        return JdbcSink.sink(sql, PostgresSinkFactory::bindDuplicateEvent, EXECUTION_OPTIONS,
            connectionOptions(jdbcUrl, username, password));
    }

    public static SinkFunction<LateEvent> silverLateEvents(String jdbcUrl, String username, String password) {
        String sql = """
            INSERT INTO silver_late_events (
                event_id, event_type, event_time, ingestion_time, user_id, session_id,
                campaign_id, ad_id, advertiser_id, category, device_type, os, country,
                placement, cost, revenue, conversion_id, conversion_value,
                attributed_click_id, watermark_time, raw_payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
            """;
        return JdbcSink.sink(sql, PostgresSinkFactory::bindLateEvent, EXECUTION_OPTIONS,
            connectionOptions(jdbcUrl, username, password));
    }

    private static void bindAdEvent(PreparedStatement statement, EventEnvelope envelope) throws SQLException {
        AdEvent event = envelope.getEvent();
        bindCommonEventFields(statement, event);
        statement.setString(20, envelope.getRawJson());
    }

    private static void bindInvalidEvent(PreparedStatement statement, InvalidEvent event) throws SQLException {
        statement.setString(1, event.getEventId());
        statement.setString(2, event.getEventType());
        setInstant(statement, 3, event.getEventTime());
        setInstant(statement, 4, event.getIngestionTime());
        statement.setString(5, event.getUserId());
        statement.setString(6, event.getCampaignId());
        statement.setString(7, event.getAdId());
        statement.setString(8, event.getAdvertiserId());
        statement.setString(9, event.getRawJson());
        statement.setString(10, joinErrors(event));
    }

    private static void bindDuplicateEvent(PreparedStatement statement, DuplicateEvent duplicateEvent) throws SQLException {
        EventEnvelope envelope = duplicateEvent.getEnvelope();
        AdEvent event = envelope.getEvent();
        statement.setString(1, event.getEventId());
        statement.setString(2, event.getEventType());
        setInstant(statement, 3, event.getEventTime());
        setInstant(statement, 4, event.getIngestionTime());
        statement.setString(5, event.getUserId());
        statement.setString(6, event.getCampaignId());
        statement.setString(7, event.getAdId());
        statement.setString(8, event.getAdvertiserId());
        statement.setString(9, event.getConversionId());
        statement.setString(10, duplicateEvent.getDuplicateKeyType());
        statement.setString(11, duplicateEvent.getDuplicateKeyValue());
        statement.setString(12, envelope.getRawJson());
    }

    private static void bindLateEvent(PreparedStatement statement, LateEvent lateEvent) throws SQLException {
        EventEnvelope envelope = lateEvent.getEnvelope();
        bindCommonEventFields(statement, envelope.getEvent());
        setInstant(statement, 20, lateEvent.getWatermarkTime());
        statement.setString(21, envelope.getRawJson());
    }

    private static void bindCommonEventFields(PreparedStatement statement, AdEvent event) throws SQLException {
        statement.setString(1, event.getEventId());
        statement.setString(2, event.getEventType());
        setInstant(statement, 3, event.getEventTime());
        setInstant(statement, 4, event.getIngestionTime());
        statement.setString(5, event.getUserId());
        statement.setString(6, event.getSessionId());
        statement.setString(7, event.getCampaignId());
        statement.setString(8, event.getAdId());
        statement.setString(9, event.getAdvertiserId());
        statement.setString(10, event.getCategory());
        statement.setString(11, event.getDeviceType());
        statement.setString(12, event.getOs());
        statement.setString(13, event.getCountry());
        statement.setString(14, event.getPlacement());
        statement.setBigDecimal(15, defaultZero(event.getCost()));
        statement.setBigDecimal(16, defaultZero(event.getRevenue()));
        statement.setString(17, event.getConversionId());
        statement.setBigDecimal(18, event.getConversionValue());
        statement.setString(19, event.getAttributedClickId());
    }

    private static JdbcConnectionOptions connectionOptions(String jdbcUrl, String username, String password) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(jdbcUrl)
            .withDriverName("org.postgresql.Driver")
            .withUsername(username)
            .withPassword(password)
            .build();
    }

    private static void setInstant(PreparedStatement statement, int index, Instant value) throws SQLException {
        if (value == null) {
            statement.setTimestamp(index, null);
            return;
        }
        statement.setTimestamp(index, Timestamp.from(value));
    }

    private static BigDecimal defaultZero(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    private static String joinErrors(InvalidEvent event) {
        StringJoiner joiner = new StringJoiner("|");
        event.getErrors().forEach(joiner::add);
        return joiner.toString();
    }
}
