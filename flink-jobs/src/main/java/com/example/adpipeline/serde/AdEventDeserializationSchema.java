package com.example.adpipeline.serde;

import com.example.adpipeline.model.AdEvent;
import com.example.adpipeline.util.JsonUtils;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class AdEventDeserializationSchema implements DeserializationSchema<AdEvent> {
    @Override
    public AdEvent deserialize(byte[] message) throws IOException {
        return JsonUtils.objectMapper().readValue(message, AdEvent.class);
    }

    @Override
    public boolean isEndOfStream(AdEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<AdEvent> getProducedType() {
        return TypeInformation.of(AdEvent.class);
    }
}
