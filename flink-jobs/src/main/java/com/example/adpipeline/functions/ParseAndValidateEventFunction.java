package com.example.adpipeline.functions;

import com.example.adpipeline.model.AdEvent;
import com.example.adpipeline.model.EventEnvelope;
import com.example.adpipeline.model.InvalidEvent;
import com.example.adpipeline.util.JsonUtils;
import com.example.adpipeline.validation.EventValidator;
import com.example.adpipeline.validation.ValidationResult;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ParseAndValidateEventFunction extends ProcessFunction<String, EventEnvelope> {
    private final OutputTag<InvalidEvent> invalidEventTag;
    private transient EventValidator validator;

    public ParseAndValidateEventFunction(OutputTag<InvalidEvent> invalidEventTag) {
        this.invalidEventTag = invalidEventTag;
    }

    @Override
    public void open(Configuration parameters) {
        this.validator = new EventValidator();
    }

    @Override
    public void processElement(String rawJson, Context context, Collector<EventEnvelope> out) {
        try {
            AdEvent event = JsonUtils.objectMapper().readValue(rawJson, AdEvent.class);
            ValidationResult result = validator.validate(event);
            if (result.valid()) {
                out.collect(new EventEnvelope(event, rawJson));
            } else {
                context.output(invalidEventTag, new InvalidEvent(event, rawJson, result.errors()));
            }
        } catch (Exception exc) {
            context.output(invalidEventTag, new InvalidEvent(rawJson, List.of(exc.getMessage())));
        }
    }
}
