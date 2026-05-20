package com.example.adpipeline.jobs;

import com.example.adpipeline.functions.CampaignMetricAggregator;
import com.example.adpipeline.functions.CampaignMetricWindowFunction;
import com.example.adpipeline.model.AdEvent;
import com.example.adpipeline.model.CampaignMetric;
import com.example.adpipeline.serde.AdEventDeserializationSchema;
import com.example.adpipeline.serde.CampaignMetricSerializationSchema;
import com.example.adpipeline.sink.ClickHouseSinkFactory;
import com.example.adpipeline.util.FlinkJobConfig;
import com.example.adpipeline.util.TimeUtils;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class CampaignMetricsJob {
    private CampaignMetricsJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrapServers = params.get("bootstrap-servers", "kafka:29092");
        String cleanTopic = params.get("clean-topic", "ad-events-clean");
        String realtimeMetricTopic = params.get("metrics-topic", "ad-metrics-realtime");
        String groupId = params.get("group-id", "campaign-metrics-job");
        String clickHouseUrl = params.get("clickhouse-url", "jdbc:clickhouse://clickhouse:8123/ad_pipeline");
        String clickHouseUser = params.get("clickhouse-user", "default");
        String clickHousePassword = params.get("clickhouse-password", "");
        boolean publishRealtimeTopic = params.getBoolean("publish-realtime-topic", true);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJobConfig.apply(env, params);

        KafkaSource<AdEvent> source = KafkaSource.<AdEvent>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(cleanTopic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new AdEventDeserializationSchema())
            .build();

        SingleOutputStreamOperator<CampaignMetric> metrics = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka ad-events-clean")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<AdEvent>forBoundedOutOfOrderness(TimeUtils.WATERMARK_DELAY)
                    .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli())
            )
            .keyBy(event -> event.getCampaignId() + "|" + event.getAdvertiserId())
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new CampaignMetricAggregator(), new CampaignMetricWindowFunction())
            .name("campaign-metrics-minutely");

        metrics.addSink(
            ClickHouseSinkFactory.goldCampaignMetricsMinutely(clickHouseUrl, clickHouseUser, clickHousePassword)
        ).name("clickhouse-gold-campaign-metrics-minutely");

        if (publishRealtimeTopic) {
            metrics.sinkTo(realtimeMetricSink(bootstrapServers, realtimeMetricTopic)).name("kafka-ad-metrics-realtime");
        }

        env.execute("Campaign Metrics Job");
    }

    private static KafkaSink<CampaignMetric> realtimeMetricSink(String bootstrapServers, String topic) {
        return KafkaSink.<CampaignMetric>builder()
            .setBootstrapServers(bootstrapServers)
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("campaign-metrics-job")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.<CampaignMetric>builder()
                    .setTopic(topic)
                    .setKeySerializationSchema((CampaignMetric metric) ->
                        (metric.getCampaignId() + "|" + metric.getAdvertiserId()).getBytes(StandardCharsets.UTF_8)
                    )
                    .setValueSerializationSchema(new CampaignMetricSerializationSchema())
                    .build()
            )
            .build();
    }
}
