package com.kafkalite.core.group;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkalite.core.config.BrokerConfig;
import com.kafkalite.core.config.BrokerConfig.FlushPolicy;
import com.kafkalite.core.group.GroupCoordinator.JoinResult;
import com.kafkalite.core.topic.TopicManager;
import com.kafkalite.protocol.model.TopicPartition;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GroupCoordinatorAndOffsetTest {

  private static final class MutableClock extends Clock {
    private final AtomicLong millis;

    MutableClock(long initialMillis) {
      this.millis = new AtomicLong(initialMillis);
    }

    void advanceMillis(long delta) {
      millis.addAndGet(delta);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(millis.get());
    }
  }

  @TempDir Path tempDir;

  @Test
  void persistsOffsetsAcrossRestart() throws Exception {
    TopicPartition tp = new TopicPartition("orders", 1);
    try (OffsetManager om = OffsetManager.open(tempDir)) {
      assertThat(om.fetchOffset("g1", tp)).isEqualTo(-1L);
      om.commitOffset("g1", tp, 42L);
    }

    try (OffsetManager recovered = OffsetManager.open(tempDir)) {
      assertThat(recovered.fetchOffset("g1", tp)).isEqualTo(42L);
    }
  }

  @Test
  void exclusiveAssignmentAndDeterministicTimeoutRebalance() throws Exception {
    MutableClock clock = new MutableClock(1_000_000L);
    BrokerConfig cfg =
        new BrokerConfig("127.0.0.1", 0, tempDir, FlushPolicy.OS_DEFAULT, 1000L, 64 * 1024L, 5000L);

    try (TopicManager tm = TopicManager.open(cfg, clock);
        GroupCoordinator coordinator = new GroupCoordinator(tm, 5000L, clock)) {
      tm.createTopic("events", 4);

      JoinResult m1 = coordinator.joinGroup("analytics", "c1", List.of("events"));
      assertThat(m1.assignment()).hasSize(4);

      JoinResult m2 = coordinator.joinGroup("analytics", "c2", List.of("events"));
      var assignments = coordinator.groupAssignments("analytics");
      assertThat(assignments.get("c1")).hasSize(2);
      assertThat(assignments.get("c2")).hasSize(2);

      // Verify zero partition overlap between c1 and c2
      Set<TopicPartition> combined = new HashSet<>(assignments.get("c1"));
      combined.addAll(assignments.get("c2"));
      assertThat(combined).hasSize(4);
      assertThat(m2.generationId()).isEqualTo(2);

      // Advance clock: c1 sends heartbeat, c2 expires
      clock.advanceMillis(4000L);
      coordinator.heartbeat("analytics", "c1", 2);
      clock.advanceMillis(2000L);

      int evicted = coordinator.evictExpiredMembers();
      assertThat(evicted).isEqualTo(1);
      assertThat(coordinator.groupAssignments("analytics")).containsOnlyKeys("c1");
      assertThat(coordinator.groupAssignments("analytics").get("c1")).hasSize(4);
    }
  }
}
