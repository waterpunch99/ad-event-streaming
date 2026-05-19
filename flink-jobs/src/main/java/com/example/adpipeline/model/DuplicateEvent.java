package com.example.adpipeline.model;

import java.io.Serializable;

public class DuplicateEvent implements Serializable {
    private EventEnvelope envelope;
    private String duplicateKeyType;
    private String duplicateKeyValue;

    public DuplicateEvent() {
    }

    public DuplicateEvent(EventEnvelope envelope, String duplicateKeyType, String duplicateKeyValue) {
        this.envelope = envelope;
        this.duplicateKeyType = duplicateKeyType;
        this.duplicateKeyValue = duplicateKeyValue;
    }

    public EventEnvelope getEnvelope() {
        return envelope;
    }

    public String getDuplicateKeyType() {
        return duplicateKeyType;
    }

    public String getDuplicateKeyValue() {
        return duplicateKeyValue;
    }
}
