package com.kafkalite.client.consumer;

import com.kafkalite.client.transport.BrokerConnection;
import com.kafkalite.protocol.model.Record;
import com.kafkalite.protocol.model.Request;
import com.kafkalite.protocol.model.Response;
import com.kafkalite.protocol.model.TopicPartition;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Single-threaded, stateful pull consumer for KafkaLite.
 * Tracks read offsets locally per partition; does not commit offsets back to the broker.
 */
public class KafkaLiteConsumer implements Closeable {

    // Bundle highWatermark with records so callers can calculate consumer lag without a second RPC
    public record FetchResult(long highWatermark, List<Record> records) {
        public FetchResult {
            // Compact constructor: ensure null-safe immutability before exposure to callers
            records = List.copyOf(Objects.requireNonNull(records, "records must not be null"));
        }
    }

    private final BrokerConnection connection;

    // Not thread-safe by design; mirroring standard Kafka consumer single-owner semantics
    private final Map<TopicPartition, Long> positions = new HashMap<>();

    public KafkaLiteConsumer(String host, int port) throws IOException {
        this.connection = BrokerConnection.connect(host, port);
    }

    /**
     * Stateless low-level fetch. Does not advance internal partition positions.
     */
    public FetchResult fetch (String topic, int partition, long offset, int maxRecords, int maxBytes) throws IOException {
        Objects.requireNonNull(topic, "topic must not be null");
        var request = new Request.Fetch(topic, partition, offset, maxRecords, maxBytes);

        // Pattern matching switch (Java 21+) ensures exhaustiveness if Response is a sealed interface
        // and surfaces protocol mismatches cleanly instead of a raw ClassCastException
        return switch (connection.send(request)) {
            case Response.Fetched fetched -> new FetchResult(fetched.highWatermark(), fetched.records());
            case Response other -> throw new IOException("Unexpected broker response for fetch: " + other);
        };
    }

    /**
     * Overrides the next fetch offset for a partition. Takes effect on the next {@link #poll}.
     */
    public void seek(TopicPartition tp, long offset) {
        Objects.requireNonNull(tp, "tp must not be null");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0: " + offset);
        }
        positions.put(tp, offset);
    }

    /**
     * Returns the next offset to be fetched. Defaults to 0 (earliest) if never polled or sought.
     */
    public long position(TopicPartition tp) {
        Objects.requireNonNull(tp, "tp must not be null");
        return positions.getOrDefault(tp, 0L);
    }

    /**
     * Stateful fetch that advances the tracked position past the last returned record.
     */
    public FetchResult poll(TopicPartition tp, int maxRecords, int maxBytes) throws IOException {
        Objects.requireNonNull(tp, "tp must not be null");
        long currentOffset = position(tp);
        FetchResult result = fetch(tp.topic(), tp.partition(), currentOffset, maxRecords, maxBytes);

        // Broker responses may skip offsets (e.g., compacted topics or control batches),
        // so advance relative to the last actual record offset rather than currentOffset + size
        if (!result.records().isEmpty()) {
            long next = result.records().getLast().offset() + 1;
            positions.put(tp, next);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        // Intentionally leave positions intact so callers can still inspect final offsets post-close
        connection.close();
    }
}
