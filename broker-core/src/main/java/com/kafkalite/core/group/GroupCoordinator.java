package com.kafkalite.core.group;

import com.kafkalite.core.topic.TopicManager;
import com.kafkalite.protocol.error.ErrorCode;
import com.kafkalite.protocol.exception.BrokerException;
import com.kafkalite.protocol.model.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Broker-side coordinator for consumer group membership, liveness heartbeats, and partition
 * assignment. Concurrency is striped per group via {@link GroupState#lock} so rebalances in one
 * group never block others.
 */
public final class GroupCoordinator implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);

  public record JoinResult(
      String groupId, String memberId, int generationId, List<TopicPartition> assignment) {
    public JoinResult {
      assignment = List.copyOf(Objects.requireNonNull(assignment, "assignment must not be null"));
    }
  }

  public record HeartbeatResult(int generationId, List<TopicPartition> assignment) {
    public HeartbeatResult {
      assignment = List.copyOf(Objects.requireNonNull(assignment, "assignment must not be null"));
    }
  }

  // Mutated strictly while holding the enclosing GroupState.lock
  private static final class MemberState {
    final String memberId;
    List<String> subscribedTopics;
    long lastHeartbeatMs;
    List<TopicPartition> assignment = List.of();

    MemberState(String memberId, List<String> subscribedTopics, long lastHeartbeatMs) {
      this.memberId = memberId;
      this.subscribedTopics = List.copyOf(subscribedTopics);
      this.lastHeartbeatMs = lastHeartbeatMs;
    }
  }

  private static final class GroupState {
    final String groupId;
    // Per-group ReentrantLock keeps group state atomic without pinning virtual threads on I/O or
    // contention
    final ReentrantLock lock = new ReentrantLock();
    final Map<String, MemberState> members = new HashMap<>();
    int generationId = 0;

    GroupState(String groupId) {
      this.groupId = groupId;
    }
  }

  private final TopicManager topicManager;
  private final long sessionTimeoutMs;
  private final Clock clock;
  private final Map<String, GroupState> groups = new ConcurrentHashMap<>();
  private final Thread reaperThread;
  private volatile boolean running = true;

  public GroupCoordinator(TopicManager topicManager, long sessionTimeoutMs, Clock clock) {
    this.topicManager = Objects.requireNonNull(topicManager, "topicManager must not be null");
    if (sessionTimeoutMs <= 0) {
      throw new IllegalArgumentException("sessionTimeoutMs must be > 0: " + sessionTimeoutMs);
    }
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");

    // Sweep at 1/3 sessionTimeoutMs (floored at 100ms) so dead members are evicted well before 2x
    // timeout
    long sweepMs = Math.max(100L, sessionTimeoutMs / 3);
    this.reaperThread =
        Thread.ofVirtual().name("group-session-readper").start(() -> runReaperLoop(sweepMs));
  }

  /**
   * Registers or refreshes a member in {@code groupId} and triggers an immediate synchronous
   * rebalance.
   */
  public JoinResult joinGroup(String groupId, String requestedMemberId, List<String> topics) {
    OffsetManager.validateGroupId(groupId);
    if (topics == null || topics.isEmpty()) {
      throw new BrokerException(ErrorCode.INVALID_REQUEST, "Subscribed topics must not be empty");
    }
    // Fail fast before creating GroupState or mutating membership if any topic does not exist
    for (String topic : topics) {
      topicManager.describeTopic(topic);
    }

    // Preserve client-supplied memberId on rejoin so the same consumer doesn't occupy two slots
    String memberId =
        (requestedMemberId == null || requestedMemberId.isBlank())
            ? "consumer-" + UUID.randomUUID()
            : requestedMemberId.trim();

    GroupState group = groups.computeIfAbsent(groupId, GroupState::new);
    group.lock.lock();
    try {
      long now = clock.millis();
      // Evict stale members first so they aren't included in this join's rebalance
      evictExpiredLocked(group, now);

      MemberState existing = group.members.get(memberId);
      if (existing == null) {
        group.members.put(memberId, new MemberState(memberId, topics, now));
      } else {
        existing.subscribedTopics = List.copyOf(topics);
        existing.lastHeartbeatMs = now;
      }
      rebalanceLocked(group);
      MemberState updated = group.members.get(memberId);
      return new JoinResult(groupId, memberId, group.generationId, updated.assignment);
    } finally {
      group.lock.unlock();
    }
  }

  /**
   * Renews a member's session lease and returns the group's latest generation and assignment.
   * Callers compare {@code generationId} against {@code clientGenerationId} to detect rebalances.
   */
  public HeartbeatResult heartbeat(String groupId, String memberId, int clientGenerationId) {
    OffsetManager.validateGroupId(groupId);
    GroupState group = groups.get(groupId);
    if (group == null) {
      throw new BrokerException(
          ErrorCode.UNKNOWN_CONSUMER_GROUP, "Unknown consumer group: " + groupId);
    }
    group.lock.lock();
    try {
      long now = clock.millis();
      // Piggyback expiry check so a slow reaper never lets an already-expired member renew its
      // lease
      evictExpiredLocked(group, now);

      MemberState member = group.members.get(memberId);
      if (member == null) {
        throw new BrokerException(
            ErrorCode.UNKNOWN_CONSUMER_GROUP,
            "Member '" + memberId + "' is not active in group " + groupId);
      }
      member.lastHeartbeatMs = now;
      return new HeartbeatResult(group.generationId, member.assignment);
    } finally {
      group.lock.unlock();
    }
  }

  /**
   * Cleanly removes a departing member and immediately reassigns its partitions without waiting for
   * timeout.
   */
  public void leaveGroup(String groupId, String memberId) {
    OffsetManager.validateGroupId(groupId);
    GroupState group = groups.get(groupId);
    if (group == null) {
      return;
    }
    group.lock.lock();
    try {
      if (group.members.remove(memberId) != null) {
        log.info("Member '{}' left group '{}'", memberId, groupId);
        rebalanceLocked(group);
      }
    } finally {
      group.lock.unlock();
    }
  }

  /**
   * Sweeps all groups and evicts any member whose heartbeat age exceeds {@link #sessionTimeoutMs}.
   */
  public int evictExpiredMembers() {
    long now = clock.millis();
    int totalEvicted = 0;
    for (GroupState group : groups.values()) {
      group.lock.lock();
      try {
        totalEvicted += evictExpiredLocked(group, now);
      } finally {
        group.lock.unlock();
      }
    }
    return totalEvicted;
  }

  /** Returns a point-in-time snapshot of current partition assignments keyed by member ID. */
  public Map<String, List<TopicPartition>> groupAssignments(String groupId) {
    OffsetManager.validateGroupId(groupId);
    GroupState group = groups.get(groupId);
    if (group == null) {
      return Map.of();
    }
    group.lock.lock();
    try {
      Map<String, List<TopicPartition>> result = new HashMap<>();
      // MemberState.assignment is already immutable (List.toList()), so shallow copy is safe
      group.members.forEach((id, m) -> result.put(id, m.assignment));
      return Map.copyOf(result);
    } finally {
      group.lock.unlock();
    }
  }

  private int evictExpiredLocked(GroupState group, long nowMs) {
    List<String> expired = new ArrayList<>();
    for (MemberState m : group.members.values()) {
      if (nowMs - m.lastHeartbeatMs > sessionTimeoutMs) {
        expired.add(m.memberId);
      }
    }
    if (expired.isEmpty()) {
      return 0;
    }
    for (String id : expired) {
      group.members.remove(id);
      log.warn("Evicted timed-out member '{}' from group '{}'", id, group.groupId);
    }
    // Only bump generation and rebalance once after batching all expirations in this sweep
    rebalanceLocked(group);
    return expired.size();
  }

  private void rebalanceLocked(GroupState group) {
    // Always bump generation on membership change, even if the group is now empty,
    // so late heartbeats from stale members never match a reused generation ID
    group.generationId++;
    if (group.members.isEmpty()) {
      return;
    }

    Set<String> allTopics = new LinkedHashSet<>();
    for (MemberState m : group.members.values()) {
      allTopics.addAll(m.subscribedTopics);
    }

    List<TopicPartition> partitions = new ArrayList<>();
    for (String topic : allTopics.stream().sorted().toList()) {
      TopicManager.TopicMetadata meta = topicManager.describeTopic(topic);
      for (int p = 0; p < meta.partitionCount(); p++) {
        partitions.add(new TopicPartition(topic, p));
      }
    }

    // Sort members deterministically so identical membership produces identical assignments across
    // runs
    List<String> sortedMembers = group.members.keySet().stream().sorted().toList();
    Map<String, List<TopicPartition>> perMember = new HashMap<>();
    for (String id : sortedMembers) {
      perMember.put(id, new ArrayList<>());
    }

    // Round-robin assignment across active members subscribed to each partition's topic
    // Note: rrCursor advances globally across partitions to spread load evenly when topics have 1
    // partition
    int rrCursor = 0;
    for (TopicPartition tp : partitions) {
      List<String> eligible =
          sortedMembers.stream()
              .filter(id -> group.members.get(id).subscribedTopics.contains(tp.topic()))
              .toList();
      if (!eligible.isEmpty()) {
        String chosen = eligible.get(rrCursor % eligible.size());
        perMember.get(chosen).add(tp);
        rrCursor++;
      }
    }

    for (String id : sortedMembers) {
      List<TopicPartition> assigned =
          perMember.get(id).stream()
              .sorted(
                  Comparator.comparing(TopicPartition::topic)
                      .thenComparingInt(TopicPartition::partition))
              .toList();
      group.members.get(id).assignment = assigned;
    }
    log.info(
        "Rebalanced group '{}' (generation={}, members={}, assignments={})",
        group.groupId,
        group.generationId,
        sortedMembers.size(),
        perMember);
  }

  private void runReaperLoop(long sweepMs) {
    while (running) {
      try {
        Thread.sleep(sweepMs);
        evictExpiredMembers();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  @Override
  public void close() throws IOException {
    running = false;
    reaperThread.interrupt();
    try {
      // Bounded join prevents broker shutdown from hanging if a group lock is temporarily contended
      reaperThread.join(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
