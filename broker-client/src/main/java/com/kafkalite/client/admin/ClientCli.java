package com.kafkalite.client.admin;

import com.kafkalite.client.consumer.KafkaLiteConsumer;
import com.kafkalite.client.producer.KafkaLiteProducer;
import com.kafkalite.protocol.model.Response;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Standalone Admin/Verification CLI (NFR-08 & Docker AC-11 crash testing). Exercises broker health
 * snapshots and end-to-end durability + offset resumption across SIGKILL restarts.
 */
public final class ClientCli {
  private static final String DEFAULT_TOPIC = "docker-crash-test";
  private static final String VERIFIER_GROUP = "crash-verifier";

  private ClientCli() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {

      System.err.println("Usage: ClientCli <host> <port> <status|seed|verify> [topic]");
      System.exit(1);
    }
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    String cmd = args[2];
    String topic = args.length > 3 ? args[3] : DEFAULT_TOPIC;

    switch (cmd) {
      case "status" -> printStatus(host, port);
      case "seed" -> seedAndCommit(host, port, topic);
      case "verify" -> verifyAfterRestart(host, port, topic);
      default -> {
        System.err.println("Unknown command: " + cmd);
        System.exit(1);
      }
    }
  }

  /**
   * Dumps broker uptime, storage footprint, crash-recovery stats, and active/idle consumer group
   * lag.
   */
  private static void printStatus(String host, int port) throws Exception {
    try (var admin = new AdminClient(host, port)) {
      Response.BrokerStatus s = admin.brokerStatus();
      System.out.printf(
          "uptimeMs=%d topics=%d partitions=%d segments=%d bytes=%d recoveryMs=%d truncatedBytes=%d groups=%d%n",
          s.uptimeMs(),
          s.topicCount(),
          s.totalPartitions(),
          s.totalSegments(),
          s.totalStorageBytes(),
          s.recoveryDurationMs(),
          s.truncatedBytesOnRecovery(),
          s.groups().size());
      s.groups()
          .forEach(g ->
              System.out.printf(
                  "  group=%s gen=%d members=%d partitions=%s%n",
                  g.groupId(), g.generationId(), g.memberCount(), g.partitions()));
    }
  }

  /**
   * Pre-crash phase: creates a 2-partition topic, publishes 4 messages (2 per partition), consumes
   * and commits 1 message per partition (2 total), leaving 2 unconsumed for post-restart
   * verification.
   */
  private static void seedAndCommit(String host, int port, String topic) throws Exception {
    try (var admin = new AdminClient(host, port)) {
      // Idempotent setup so re-running seed against an existing broker doesn't fail on
      // TOPIC_ALREADY_EXISTS
      if (admin.listTopics().stream().noneMatch(t -> t.name().equals(topic))) {
        admin.createTopic(topic, 2);
      }
    }
    try (var producer = new KafkaLiteProducer(host, port)) {
      // Explicitly stripe 2 records into partition 0 and 2 into partition 1 so round-robin poll
      // deterministically reads 1 record from each partition before committing
      for (int i = 0; i < 4; i++) {
        producer.send(topic, i % 2, null, ("msg-" + i).getBytes(StandardCharsets.UTF_8));
      }
    }
    try (var consumer =
        new KafkaLiteConsumer(
            host,
            port,
            VERIFIER_GROUP,
            "v1",
            KafkaLiteConsumer.OffsetResetStrategy.EARLIEST,
            500L)) {
      consumer.subscribe(List.of(topic));
      List<KafkaLiteConsumer.ConsumedRecord> firstTwo = consumer.poll(2, 64 * 1024);
      if (firstTwo.size() != 2) {
        throw new IllegalStateException("Expected 2 initial records, got " + firstTwo.size());
      }
      // Persists offset=1 on both partitions to offsets.meta before the caller SIGKILLs the broker
      consumer.commitSync();
    }
    System.out.println("Seeded 4 records and committed first 2");
  }

  /**
   * Post-crash phase: rejoins the same consumer group under a fresh memberId and confirms the
   * broker replays only the 2 uncommitted records (offset=1 on each partition) without duplicates.
   */
  private static void verifyAfterRestart(String host, int port, String topic) throws Exception {
    try (var consumer =
        new KafkaLiteConsumer(
            host,
            port,
            VERIFIER_GROUP,
            "v2",
            KafkaLiteConsumer.OffsetResetStrategy.EARLIEST,
            500L)) {
      consumer.subscribe(List.of(topic));
      List<KafkaLiteConsumer.ConsumedRecord> remaining = consumer.poll(10, 64 * 1024);
      if (remaining.size() != 2) {
        throw new IllegalStateException(
            "Expected 2 uncommitted records after SIGKILL, got " + remaining.size());
      }
    }
    System.out.println("Verified durability and committed offset resumption after SIGKILL.");
  }
}
