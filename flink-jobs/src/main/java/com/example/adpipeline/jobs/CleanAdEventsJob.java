package com.example.adpipeline.jobs;

import com.example.adpipeline.functions.EventDeduplicator;
import com.example.adpipeline.functions.LateEventRouter;
import com.example.adpipeline.functions.ParseAndValidateEventFunction;
import com.example.adpipeline.model.DuplicateEvent;
import com.example.adpipeline.model.EventEnvelope;
import com.example.adpipeline.model.InvalidEvent;
import com.example.adpipeline.model.LateEvent;
import com.example.adpipeline.serde.EventEnvelopeKeySerializationSchema;
import com.example.adpipeline.serde.EventEnvelopeSerializationSchema;
import com.example.adpipeline.sink.PostgresSinkFactory;
import com.example.adpipeline.util.TimeUtils;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public final class CleanAdEventsJob {
    private static final OutputTag<InvalidEvent> INVALID_EVENTS = new OutputTag<>("invalid-events") {
    };
    private static final OutputTag<LateEvent> LATE_EVENTS = new OutputTag<>("late-events") {
    };
    private static final OutputTag<DuplicateEvent> DUPLICATE_EVENTS = new OutputTag<>("duplicate-events") {
    };

    private CleanAdEventsJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrapServers = params.get("bootstrap-servers", "kafka:29092");
        String rawTopic = params.get("raw-topic", "ad-events-raw");
        String cleanTopic = params.get("clean-topic", "ad-events-clean");
        String groupId = params.get("group-id", "clean-ad-events-job");
        String postgresUrl = params.get("postgres-url", "jdbc:postgresql://postgres:5432/ad_pipeline");
        String postgresUser = params.get("postgres-user", "postgres");
        String postgresPassword = params.get("postgres-password", "postgres");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(Duration.ofMinutes(1).toMillis());

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(rawTopic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawEvents = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka ad-events-raw");

        SingleOutputStreamOperator<EventEnvelope> validEvents = rawEvents
            .process(new ParseAndValidateEventFunction(INVALID_EVENTS))
            .name("parse-and-validate-events");

        DataStream<InvalidEvent> invalidEvents = validEvents.getSideOutput(INVALID_EVENTS);

        SingleOutputStreamOperator<EventEnvelope> nonLateEvents = validEvents
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<EventEnvelope>forBoundedOutOfOrderness(TimeUtils.WATERMARK_DELAY)
                    .withTimestampAssigner((envelope, timestamp) -> envelope.getEvent().getEventTime().toEpochMilli())
            )
            .process(new LateEventRouter(LATE_EVENTS))
            .name("route-late-events");

        DataStream<LateEvent> lateEvents = nonLateEvents.getSideOutput(LATE_EVENTS);

        SingleOutputStreamOperator<EventEnvelope> eventIdUniqueEvents = nonLateEvents
            .keyBy(EventEnvelope::eventId)
            .process(new EventDeduplicator(EventDeduplicator.EVENT_ID_KEY, DUPLICATE_EVENTS))
            .name("deduplicate-by-event-id");

        DataStream<DuplicateEvent> eventIdDuplicates = eventIdUniqueEvents.getSideOutput(DUPLICATE_EVENTS);

        SingleOutputStreamOperator<EventEnvelope> cleanEvents = eventIdUniqueEvents
            .keyBy(EventEnvelope::conversionDedupKey)
            .process(new EventDeduplicator(EventDeduplicator.CONVERSION_ID_KEY, DUPLICATE_EVENTS))
            .name("deduplicate-by-conversion-id");

        DataStream<DuplicateEvent> conversionDuplicates = cleanEvents.getSideOutput(DUPLICATE_EVENTS);
        DataStream<DuplicateEvent> duplicateEvents = eventIdDuplicates.union(conversionDuplicates);

        cleanEvents.addSink(PostgresSinkFactory.silverAdEvents(postgresUrl, postgresUser, postgresPassword))
            .name("postgres-silver-ad-events");
        invalidEvents.addSink(PostgresSinkFactory.silverInvalidEvents(postgresUrl, postgresUser, postgresPassword))
            .name("postgres-silver-invalid-events");
        duplicateEvents.addSink(PostgresSinkFactory.silverDuplicateEvents(postgresUrl, postgresUser, postgresPassword))
            .name("postgres-silver-duplicate-events");
        lateEvents.addSink(PostgresSinkFactory.silverLateEvents(postgresUrl, postgresUser, postgresPassword))
            .name("postgres-silver-late-events");

        cleanEvents.sinkTo(cleanKafkaSink(bootstrapServers, cleanTopic)).name("kafka-ad-events-clean");

        env.execute("Clean Ad Events Job");
    }

    private static KafkaSink<EventEnvelope> cleanKafkaSink(String bootstrapServers, String cleanTopic) {
        return KafkaSink.<EventEnvelope>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.<EventEnvelope>builder()
                    .setTopic(cleanTopic)
                    .setKeySerializationSchema(new EventEnvelopeKeySerializationSchema())
                    .setValueSerializationSchema(new EventEnvelopeSerializationSchema())
                    .build()
            )
            .build();
    }
}
