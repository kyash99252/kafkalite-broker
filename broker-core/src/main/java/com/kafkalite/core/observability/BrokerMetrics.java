package com.kafkalite.core.observability;

import com.kafkalite.core.group.GroupCoordinator;
import com.kafkalite.core.group.OffsetManager;
import com.kafkalite.core.topic.TopicManager;
import com.kafkalite.protocol.model.Response;
import com.kafkalite.protocol.model.Response.BrokerStatus;
import com.kafkalite.protocol.model.TopicPartition;
import java.time.Clock;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Read-only aggregator that stitches together storage stats, live group membership, and durable
 * offset state into a point-in-time {@link BrokerStatus} snapshot. Best-effort consistency:
 * sub-components are queried sequentially without a global lock.
 */
public final class BrokerMetrics {
  private final TopicManager topicManager;
  private final GroupCoordinator groupCoordinator;
  private final OffsetManager offsetManager;
  private final Clock clock;
  private final long startedAtMs;

  public BrokerMetrics(
      TopicManager topicManager,
      GroupCoordinator groupCoordinator,
      OffsetManager offsetManager,
      Clock clock) {
    this.topicManager = Objects.requireNonNull(topicManager, "topicManager must not be null");
    this.groupCoordinator =
        Objects.requireNonNull(groupCoordinator, "groupCoordinator must not be null");
    this.offsetManager = Objects.requireNonNull(offsetManager, "offsetManager must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.startedAtMs = clock.millis();
  }

  /**
   * Collects a point-in-time health and consumer-lag snapshot across all known topics and groups.
   */
  public Response.BrokerStatus snapshot() {
    TopicManager.StorageStats storage = topicManager.storageStats();

    // Index topic metadata once so per-partition lag lookups below stay O(1)
    Map<String, TopicManager.TopicMetadata> topicMetaMap =
        topicManager.listTopics().stream()
            .collect(
                Collectors.toMap(
                    TopicManager.TopicMetadata::name,
                    Function.identity(),
                    (existing, replacement) -> replacement,
                    LinkedHashMap::new));

    Map<String, GroupCoordinator.GroupSummary> activeByGroup =
        groupCoordinator.groupSummaries().stream()
            .collect(
                Collectors.toMap(
                    GroupCoordinator.GroupSummary::groupId,
                    Function.identity(),
                    (existing, replacement) -> replacement,
                    LinkedHashMap::new));

    // Union live groups with idle/offline groups that still have durable offsets on disk
    Set<String> allGroups = new LinkedHashSet<>(activeByGroup.keySet());
    allGroups.addAll(offsetManager.knownGroupIds());

    List<Response.GroupStatusInfo> groupInfos = new ArrayList<>(allGroups.size());
    for (String groupId : allGroups) {
      GroupCoordinator.GroupSummary active = activeByGroup.get(groupId);
      Map<TopicPartition, Long> committedMap = offsetManager.groupOffsets(groupId);

      // Include both previously committed partitions and newly assigned partitions with no commits
      // yet
      Set<TopicPartition> tracked = new LinkedHashSet<>(committedMap.keySet());
      if (active != null) {
        tracked.addAll(active.assignedPartitions());
      }

      List<Response.ParitionLag> lags = new ArrayList<>(tracked.size());
      for (TopicPartition tp : tracked) {
        TopicManager.TopicMetadata meta = topicMetaMap.get(tp.topic());
        // Skip partitions belonging to deleted topics or invalid indices
        if (meta != null && tp.partition() >= 0 && tp.partition() < meta.partitionCount()) {
          long hwm = meta.endOffsets().get(tp.partition());
          long committed = committedMap.getOrDefault(tp, -1L);
          // Uncommitted (-1) treats the entire partition up to hwm as backlog
          // clamp at 0 in case a concurrent reset/truncation briefly puts committed > hwm
          long lag = committed < 0 ? hwm : Math.max(0L, hwm - committed);
          lags.add(new Response.ParitionLag(tp.topic(), tp.partition(), hwm, committed, lag));
        }
      }

      // Deterministic ordering makes CLI/metrics output and snapshot assertions stable
      lags.sort(
          Comparator.comparing(Response.ParitionLag::topic)
              .thenComparingInt(Response.ParitionLag::partition));

      groupInfos.add(
          new Response.GroupStatusInfo(
              groupId,
              active == null ? 0 : active.generationId(),
              active == null ? 0 : active.memberCount(),
              lags));
    }
    // Guard against non-monotonic wall-clock adjustments (e.g., NTP step backwards)
    long uptimeMs = Math.max(0L, clock.millis() - startedAtMs);

    return new Response.BrokerStatus(
        uptimeMs,
        storage.topicCount(),
        storage.totalPartitions(),
        storage.totalSegments(),
        storage.totalStorageBytes(),
        storage.recoveryDurationMs(),
        storage.truncatedBytesOnRecovery(),
        groupInfos);
  }
}
