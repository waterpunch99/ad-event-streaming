package com.example.adpipeline.functions;

import com.example.adpipeline.model.EventEnvelope;
import com.example.adpipeline.model.LateEvent;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LateEventRouter extends ProcessFunction<EventEnvelope, EventEnvelope> {
    private final OutputTag<LateEvent> lateEventTag;

    public LateEventRouter(OutputTag<LateEvent> lateEventTag) {
        this.lateEventTag = lateEventTag;
    }

    @Override
    public void processElement(EventEnvelope envelope, Context context, Collector<EventEnvelope> out) {
        long currentWatermark = context.timerService().currentWatermark();
        long eventTime = envelope.getEvent().getEventTime().toEpochMilli();

        if (currentWatermark != Long.MIN_VALUE && eventTime <= currentWatermark) {
            context.output(lateEventTag, new LateEvent(envelope, Instant.ofEpochMilli(currentWatermark)));
            return;
        }

        out.collect(envelope);
    }
}
