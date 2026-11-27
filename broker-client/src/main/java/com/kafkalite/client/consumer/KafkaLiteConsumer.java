package com.kafkalite.client.consumer;

import com.kafkalite.client.transport.BrokerConnection;
import com.kafkalite.protocol.exception.BrokerException;
import com.kafkalite.protocol.model.Record;
import com.kafkalite.protocol.model.Request;
import com.kafkalite.protocol.model.Response;
import com.kafkalite.protocol.model.TopicPartition;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-threaded, stateful pull consumer for KafkaLite. Tracks read offsets locally per partition;
 * does not commit offsets back to the broker.
 */
public class KafkaLiteConsumer implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(KafkaLiteConsumer.class);

  public enum OffsetResetStrategy {
    EARLIEST,
    LATEST
  }

  // Bundle highWatermark with records so callers can calculate consumer lag without a second RPC
  public record FetchResult(long highWatermark, List<Record> records) {
    public FetchResult {
      // Compact constructor: ensure null-safe immutability before exposure to callers
      records = List.copyOf(Objects.requireNonNull(records, "records must not be null"));
    }
  }

  public record ConsumedRecord(String topic, int partition, Record record) {
    public ConsumedRecord {
      Objects.requireNonNull(topic, "topic must not be null");
      Objects.requireNonNull(record, "record must not be null");
    }
  }

  // Immutable pair so background heartbeats publish generation + assignment updates atomically
  private record GroupState(int generationId, List<TopicPartition> assignment) {
    GroupState {
      assignment = List.copyOf(Objects.requireNonNull(assignment, "assignment must not be null"));
    }
  }

  private final BrokerConnection connection;
  private final String groupId;
  private final OffsetResetStrategy resetStrategy;
  private final long heartbeatIntervalMs;
  // Not thread-safe by design; mirroring standard Kafka consumer single-owner semantics
  private final Map<TopicPartition, Long> positions = new HashMap<>();

  // Tracks which group generation's partition assignment has been reconciled into local positions
  private int appliedGeneration = 0;
  private volatile boolean heartbeatActive;
  private Thread heartbeatThread;

  // Rotates starting partition across group poll() calls so the first partition never starves later
  // ones
  private int rrCursor = 0;

  private List<String> subscribedTopics = List.of();
  private String memberId;
  private volatile GroupState latestGroupState = new GroupState(0, List.of());

  public KafkaLiteConsumer(String host, int port) throws IOException {
    this(host, port, null, null, OffsetResetStrategy.EARLIEST, 1000L);
  }

  public KafkaLiteConsumer(
      String host,
      int port,
      String groupId,
      String memberId,
      OffsetResetStrategy resetStrategy,
      long heartbeatIntervalMs)
      throws IOException {
    if (heartbeatIntervalMs <= 0) {
      throw new IllegalArgumentException("heartbeatIntervalMs must be > 0: " + heartbeatIntervalMs);
    }
    this.connection = BrokerConnection.connect(host, port);
    this.groupId = groupId;
    this.memberId = memberId;
    this.resetStrategy = Objects.requireNonNull(resetStrategy, "resetStrategy must not be null");
    this.heartbeatIntervalMs = heartbeatIntervalMs;
  }

  /** Stateless low-level fetch. Does not advance internal partition positions. */
  public FetchResult fetch(String topic, int partition, long offset, int maxRecords, int maxBytes)
      throws IOException {
    Objects.requireNonNull(topic, "topic must not be null");
    var request = new Request.Fetch(topic, partition, offset, maxRecords, maxBytes);

    // Pattern matching switch (Java 21+) ensures exhaustiveness if Response is a sealed interface
    // and surfaces protocol mismatches cleanly instead of a raw ClassCastException
    return switch (connection.send(request)) {
      case Response.Fetched fetched -> new FetchResult(fetched.highWatermark(), fetched.records());
      case Response other ->
          throw new IOException("Unexpected broker response for fetch: " + other);
    };
  }

  /** Overrides the next fetch offset for a partition. Takes effect on the next {@link #poll}. */
  public void seek(TopicPartition tp, long offset) {
    Objects.requireNonNull(tp, "tp must not be null");
    if (offset < 0) {
      throw new IllegalArgumentException("offset must be >= 0: " + offset);
    }
    positions.put(tp, offset);
  }

  /** Returns the next offset to be fetched. Defaults to 0 (earliest) if never polled or sought. */
  public long position(TopicPartition tp) {
    Objects.requireNonNull(tp, "tp must not be null");
    return positions.getOrDefault(tp, 0L);
  }

  /** Stateful fetch that advances the tracked position past the last returned record. */
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

  /**
   * Joins the consumer group, initializes positions for assigned partitions, and starts background
   * heartbeats.
   */
  public List<TopicPartition> subscribe(List<String> topics) throws IOException {
    if (groupId == null) {
      throw new IllegalStateException("Consumer mut be created with a groupId to subscribe");
    }
    Objects.requireNonNull(topics, "topics must not be null");
    this.subscribedTopics = List.copyOf(topics);

    Response.GroupJoined joined =
        switch (connection.send(new Request.JoinGroup(groupId, memberId, subscribedTopics))) {
          case Response.GroupJoined gj -> gj;
          case Response other ->
              throw new IOException("Unexpected broker rsponse for JoinGroup: " + other);
        };
    this.memberId = joined.memberId();
    this.latestGroupState = new GroupState(joined.generationId(), joined.assignment());
    syncPositionsWithAssignment(joined.generationId(), joined.assignment());
    startHeartbeatIfNeeded();
    return joined.assignment();
  }

  /** Group-coordinated poll across all currently assigned partitions using round-robin fairness. */
  public List<ConsumedRecord> poll(int maxRecords, int maxBytes) throws IOException {
    if (groupId == null) {
      throw new IllegalStateException("Use poll(TopicPartition, ...) when not in group");
    }

    // Reconcile on the poll thread rather than the heartbeat thread so positions stays
    // single-threaded
    GroupState state = latestGroupState;
    if (state.generationId() != appliedGeneration) {
      syncPositionsWithAssignment(state.generationId(), state.assignment());
    }

    List<TopicPartition> assigned = state.assignment();
    if (assigned.isEmpty() || maxRecords <= 0) {
      return List.of();
    }

    List<ConsumedRecord> results = new ArrayList<>();
    int remainingRecords = maxRecords;
    int remainingBytes = maxBytes;

    for (int i = 0; i < assigned.size() && remainingRecords > 0 && remainingBytes > 0; i++) {
      TopicPartition tp = assigned.get((rrCursor + 1) % assigned.size());
      long offset = positions.getOrDefault(tp, 0L);
      FetchResult fetched =
          fetch(tp.topic(), tp.partition(), offset, remainingRecords, remainingBytes);
      for (Record r : fetched.records()) {
        results.add(new ConsumedRecord(tp.topic(), tp.partition(), r));
        remainingRecords--;
        int sz = (r.key() == null ? 0 : r.key().length) + r.payload().length;
        remainingBytes = Math.max(0, remainingBytes - sz);
      }
      if (!fetched.records().isEmpty()) {
        positions.put(tp, fetched.records().getLast().offset() + 1);
      }
    }
    rrCursor = (rrCursor + 1) % assigned.size();
    return results;
  }

  /** Synchronously commits the next-to-fetch offset for every currently assigned partition. */
  public void commitSync() throws IOException {
    if (groupId == null) {
      throw new IllegalStateException("Consumer must belong to a group to commit offsets");
    }
    for (TopicPartition tp : latestGroupState.assignment()) {
      Long pos = positions.get(tp);
      if (pos != null) {
        connection.send(new Request.CommitOffset(groupId, tp, pos));
      }
    }
  }

  public void commitSync(TopicPartition tp, long offset) throws IOException {
    if (groupId == null) {
      throw new IllegalStateException("Consumer must belong to a group to commit offsets");
    }
    Objects.requireNonNull(tp, "tp must not be null");
    connection.send(new Request.CommitOffset(groupId, tp, offset));
  }

  /** Fetches the last broker-committed offset for {@code tp}, or -1 if none exists yet. */
  public long committed(TopicPartition tp) throws IOException {
    if (groupId == null) {
      throw new IllegalStateException("Consumer must belong to a group to query offsets");
    }
    Objects.requireNonNull(tp, "tp must not be null");
    return switch (connection.send(new Request.FetchOffset(groupId, tp))) {
      case Response.OffsetFetched fetched -> fetched.committedOffset();
      case Response other ->
          throw new IOException("Unexpected broker response for FetchOffset: " + other);
    };
  }

  public List<TopicPartition> assignment() {
    return latestGroupState.assignment();
  }

  private void syncPositionsWithAssignment(int generationId, List<TopicPartition> assignment)
      throws IOException {
    // Drop revoked partitions so stale offsets aren't kept if the partition is later reassigned
    // back
    Set<TopicPartition> active = new HashSet<>(assignment);
    positions.keySet().retainAll(active);

    for (TopicPartition tp : assignment) {
      if (!positions.containsKey(tp)) {
        long committed = committed(tp);
        if (committed >= 0) {
          positions.put(tp, committed);
        } else if (resetStrategy == OffsetResetStrategy.EARLIEST) {
          positions.put(tp, 0L);
        } else {
          // LATEST: start at the broker's current end offset so pre-existing backlog is skipped
          Response.TopicDescribed desc =
              switch (connection.send(new Request.DescribeTopic(tp.topic()))) {
                case Response.TopicDescribed td -> td;
                case Response other ->
                    throw new IOException("Unexpected broker response for DescribeTopic: " + other);
              };
          positions.put(tp, desc.topic().endOffsets().get(tp.partition()));
        }
      }
    }
    appliedGeneration = generationId;
  }

  private void startHeartbeatIfNeeded() {
    if (heartbeatActive) {
      return;
    }
    heartbeatActive = true;
    heartbeatThread =
        Thread.ofVirtual()
            .name("consumer-heartbeat-" + memberId)
            .start(() -> {
              while (heartbeatActive) {
                try {
                  Thread.sleep(heartbeatIntervalMs);
                  if (!heartbeatActive) {
                    break;
                  }
                  GroupState cur = latestGroupState;
                  Response.HeartbeatAck ack =
                      switch (connection.send(
                          new Request.Heartbeat(groupId, memberId, cur.generationId()))) {
                        case Response.HeartbeatAck h -> h;
                        case Response other ->
                            throw new IOException(
                                "Unexpected broker response for Heartbeat: " + other);
                      };
                  // GroupCoordinator piggybacks rebalance updates onto HeartbeatAck;
                  // publish atomically for the next poll() to reconcile
                  if (ack.generationId() != cur.generationId()) {
                    latestGroupState = new GroupState(ack.generationId(), ack.assignment());
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (BrokerException | IOException e) {
                  // Exit quietly on session eviction or socket close; next poll/join surfaces
                  // errors
                  if (heartbeatActive) {
                    log.debug("Heartbeat stopped for {}: {}", memberId, e.getMessage());
                  }
                  break;
                }
              }
            });
  }

  @Override
  public void close() throws IOException {
    heartbeatActive = false;
    if (heartbeatThread != null) {
      heartbeatThread.interrupt();
      try {
        heartbeatThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    // Best-effort LeaveGroup triggers an immediate rebalance on the broker instead of waiting for
    // session timeout
    if (groupId != null && memberId != null) {
      try {
        connection.send(new Request.LeaveGroup(groupId, memberId));
      } catch (IOException | BrokerException e) {
        log.debug("LeaveGroup failed during close (connection may already be closed)", e);
      }
    }
    // Intentionally leave positions intact so callers can still inspect final offsets post-close
    connection.close();
  }
}
