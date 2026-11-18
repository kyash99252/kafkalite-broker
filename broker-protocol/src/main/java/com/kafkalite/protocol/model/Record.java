package com.kafkalite.protocol.model;

import java.util.Arrays;
import java.util.Objects;

public record Record(long offset, long timestamp, byte[] key, byte[] payload) {
    public Record {
        Objects.requireNonNull(payload, "payload must not be null");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record other)) return false;
        return offset == other.offset
                && timestamp == other.timestamp
                && Arrays.equals(key, other.key)
                && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "Record[offset="
                + offset
                + ", timestamp="
                + timestamp
                + ", keyBytes="
                + (key == null ? "null" : key.length)
                + ", payloadBytes="
                + payload.length
                + "]";
    }
}
