package com.example.adpipeline.functions;

import com.example.adpipeline.model.DuplicateEvent;
import com.example.adpipeline.model.EventEnvelope;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class EventDeduplicator extends KeyedProcessFunction<String, EventEnvelope, EventEnvelope> {
    public static final String EVENT_ID_KEY = "event_id";
    public static final String CONVERSION_ID_KEY = "conversion_id";

    private final String duplicateKeyType;
    private final OutputTag<DuplicateEvent> duplicateEventTag;
    private final long ttlHours;
    private transient ValueState<Boolean> seen;

    public EventDeduplicator(String duplicateKeyType, OutputTag<DuplicateEvent> duplicateEventTag, long ttlHours) {
        this.duplicateKeyType = duplicateKeyType;
        this.duplicateEventTag = duplicateEventTag;
        this.ttlHours = ttlHours;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> descriptor =
            new ValueStateDescriptor<>("seen-" + duplicateKeyType, Boolean.class);
        descriptor.enableTimeToLive(
            StateTtlConfig
                .newBuilder(Time.hours(ttlHours))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build()
        );
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(EventEnvelope envelope, Context context, Collector<EventEnvelope> out) throws Exception {
        String duplicateKeyValue = context.getCurrentKey();

        if (Boolean.TRUE.equals(seen.value())) {
            context.output(duplicateEventTag, new DuplicateEvent(envelope, duplicateKeyType, duplicateKeyValue));
            return;
        }

        seen.update(true);
        out.collect(envelope);
    }
}
