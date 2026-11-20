package com.kafkalite.protocol.model;

import java.util.Objects;

public record TopicPartition(String topic, int partition) {
    public TopicPartition {
        Objects.requireNonNull(topic, "topic must not be null");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0: " + partition);
        }
    }
}
