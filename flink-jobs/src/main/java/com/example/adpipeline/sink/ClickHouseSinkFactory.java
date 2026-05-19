package com.example.adpipeline.sink;

import com.example.adpipeline.model.CampaignMetric;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public final class ClickHouseSinkFactory {
    private static final JdbcExecutionOptions EXECUTION_OPTIONS = JdbcExecutionOptions.builder()
        .withBatchSize(500)
        .withBatchIntervalMs(1000)
        .withMaxRetries(3)
        .build();

    private ClickHouseSinkFactory() {
    }

    public static SinkFunction<CampaignMetric> goldCampaignMetricsMinutely(
        String jdbcUrl,
        String username,
        String password
    ) {
        String sql = """
            INSERT INTO gold_campaign_metrics_minutely (
                window_start, window_end, campaign_id, advertiser_id,
                impressions, clicks, conversions, ctr, cvr, cost, revenue, roas
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        return JdbcSink.sink(sql, ClickHouseSinkFactory::bindCampaignMetric, EXECUTION_OPTIONS,
            connectionOptions(jdbcUrl, username, password));
    }

    private static void bindCampaignMetric(PreparedStatement statement, CampaignMetric metric) throws SQLException {
        statement.setTimestamp(1, Timestamp.from(metric.getWindowStart()));
        statement.setTimestamp(2, Timestamp.from(metric.getWindowEnd()));
        statement.setString(3, metric.getCampaignId());
        statement.setString(4, metric.getAdvertiserId());
        statement.setLong(5, metric.getImpressions());
        statement.setLong(6, metric.getClicks());
        statement.setLong(7, metric.getConversions());
        statement.setDouble(8, metric.getCtr());
        statement.setDouble(9, metric.getCvr());
        statement.setBigDecimal(10, metric.getCost());
        statement.setBigDecimal(11, metric.getRevenue());
        statement.setDouble(12, metric.getRoas());
    }

    private static JdbcConnectionOptions connectionOptions(String jdbcUrl, String username, String password) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(jdbcUrl)
            .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
            .withUsername(username)
            .withPassword(password)
            .build();
    }
}
