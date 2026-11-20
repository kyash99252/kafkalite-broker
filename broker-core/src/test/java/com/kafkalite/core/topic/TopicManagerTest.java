package com.kafkalite.core.topic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkalite.core.config.BrokerConfig;
import com.kafkalite.core.config.BrokerConfig.FlushPolicy;
import com.kafkalite.core.topic.TopicManager.ProduceResult;
import com.kafkalite.core.topic.TopicManager.TopicMetadata;
import com.kafkalite.protocol.error.ErrorCode;
import com.kafkalite.protocol.exception.BrokerException;
import com.kafkalite.protocol.model.Record;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopicManagerTest {
  private static final Clock FIXED_CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

  @TempDir Path tempDir;

  private BrokerConfig testConfig() {
    return new BrokerConfig(
        "127.0.0.1", 0, tempDir, FlushPolicy.EVERY_WRITE, 1000L, 64 * 1024L, 10_000L);
  }

  @Test
  @Tag("AC-01")
  void createTopicValidatesInputsAndRejectsDuplicates() throws Exception {
    try (TopicManager manager = TopicManager.open(testConfig(), FIXED_CLOCK)) {
      TopicMetadata meta = manager.createTopic("orders", 3);
      assertThat(meta.name()).isEqualTo("orders");
      assertThat(meta.partitionCount()).isEqualTo(3);

      assertThatThrownBy(() -> manager.createTopic("orders", 3))
          .isInstanceOf(BrokerException.class)
          .extracting(e -> ((BrokerException) e).errorCode())
          .isEqualTo(ErrorCode.TOPIC_ALREADY_EXISTS);

      assertThatThrownBy(() -> manager.createTopic("bad topic!", 2))
          .isInstanceOf(BrokerException.class)
          .extracting(e -> ((BrokerException) e).errorCode())
          .isEqualTo(ErrorCode.INVALID_REQUEST);

      assertThatThrownBy(() -> manager.createTopic("valid", 0))
          .isInstanceOf(BrokerException.class)
          .extracting(e -> ((BrokerException) e).errorCode())
          .isEqualTo(ErrorCode.INVALID_REQUEST);
    }
  }

  @Test
  @Tag("AC-02")
  void keyedMessagesRouteConsistentlyAndUnkeyedDistributeRoundRobin() throws Exception {
    try (TopicManager manager = TopicManager.open(testConfig(), FIXED_CLOCK)) {
      manager.createTopic("events", 3);

      byte[] key = "user-42".getBytes(StandardCharsets.UTF_8);
      Set<Integer> keyedPartitions = new HashSet<>();
      for (int i = 0; i < 5; i++) {
        ProduceResult res =
            manager.publish("events", null, key, ("ev-" + i).getBytes(StandardCharsets.UTF_8));
        keyedPartitions.add(res.partition());
      }
      assertThat(keyedPartitions).hasSize(1);

      Set<Integer> roundRobinPartitions = new HashSet<>();
      for (int i = 0; i < 3; i++) {
        ProduceResult res =
            manager.publish("events", null, null, ("rr-" + i).getBytes(StandardCharsets.UTF_8));
        roundRobinPartitions.add(res.partition());
      }
      assertThat(roundRobinPartitions).containsExactlyInAnyOrder(0, 1, 2);
    }
  }

  @Test
  @Tag("AC-03")
  void recoversTopicsAndMessagesAcrossRestart() throws Exception {
    try (TopicManager manager = TopicManager.open(testConfig(), FIXED_CLOCK)) {
      manager.createTopic("payments", 2);
      manager.publish("payments", 0, null, "p0-msg0".getBytes(StandardCharsets.UTF_8));
      manager.publish("payments", 1, null, "p1-msg0".getBytes(StandardCharsets.UTF_8));
    }

    try (TopicManager recovered = TopicManager.open(testConfig(), FIXED_CLOCK)) {
      TopicMetadata meta = recovered.describeTopic("payments");
      assertThat(meta.partitionCount()).isEqualTo(2);
      assertThat(meta.endOffsets()).containsExactly(1L, 1L);

      List<Record> p0 = recovered.fetch("payments", 0, 0L, 10, 64 * 1024);
      assertThat(p0).hasSize(1);
      assertThat(new String(p0.getFirst().payload(), StandardCharsets.UTF_8)).isEqualTo("p0-msg0");
    }
  }
}
