package com.kafkalite.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkalite.client.admin.AdminClient;
import com.kafkalite.client.consumer.KafkaLiteConsumer;
import com.kafkalite.client.consumer.KafkaLiteConsumer.ConsumedRecord;
import com.kafkalite.client.consumer.KafkaLiteConsumer.OffsetResetStrategy;
import com.kafkalite.client.producer.KafkaLiteProducer;
import com.kafkalite.protocol.model.Response.BrokerStatus;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CrashRecoveryE2EIT {
  @TempDir Path tempDir;

  @Test
  @Tag("AC-09")
  @Tag("AC-10")
  void childJvmSigkillAndCorruptedTailRecoveryWithConcurrentStress() throws Exception {
    int port = findFreePort();
    Process broker = startBrokerProcess(port, tempDir);
    try {
      waitForBrokerReady(port);

      try (AdminClient admin = new AdminClient("127.0.0.1", port)) {
        admin.createTopic("resilient", 2);
      }

      // Concurrent virtual-thread producer stress (AC-10)
      try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        List<Future<?>> producers = new ArrayList<>();
        for (int p = 0; p < 4; p++) {
          final int worker = p;
          producers.add(
              executor.submit(() -> {
                try (KafkaLiteProducer producer = new KafkaLiteProducer("127.0.0.1", port)) {
                  for (int i = 0; i < 25; i++) {
                    producer.send(
                        "resilient",
                        worker % 2,
                        null,
                        ("w" + worker + "-m" + i).getBytes(StandardCharsets.UTF_8));
                  }
                }
                return null;
              }));
        }
        for (Future<?> f : producers) {
          f.get();
        }
      }

      // Consume and commit 50 of the 100 messages before crashing
      try (KafkaLiteConsumer consumer =
          new KafkaLiteConsumer(
              "127.0.0.1", port, "recovery-group", "c1", OffsetResetStrategy.EARLIEST, 500L)) {
        consumer.subscribe(List.of("resilient"));
        List<ConsumedRecord> firstHalf = consumer.poll(50, 1024 * 1024);
        assertThat(firstHalf).hasSize(50);
        consumer.commitSync();
      }
    } finally {
      // Simulate ungraceful SIGKILL
      broker.destroyForcibly();
      broker.waitFor(5, TimeUnit.SECONDS);
    }

    // Simulate torn trailing write on partition 0's active log segment
    Path seg0 = tempDir.resolve("resilient").resolve("0").resolve("00000000000000000000.log");
    try (FileChannel ch = FileChannel.open(seg0, StandardOpenOption.WRITE)) {
      ch.position(ch.size());
      ch.write(ByteBuffer.wrap(new byte[] {0, 0, 0, 120, 1, 2, 3}));
    }

    // Restart broker process and verify recovery + observability + remaining messages
    Process restarted = startBrokerProcess(port, tempDir);
    try {
      waitForBrokerReady(port);

      try (AdminClient admin = new AdminClient("127.0.0.1", port);
          KafkaLiteConsumer consumer =
              new KafkaLiteConsumer(
                  "127.0.0.1", port, "recovery-group", "c2", OffsetResetStrategy.EARLIEST, 500L)) {
        BrokerStatus status = admin.brokerStatus();
        assertThat(status.topicCount()).isEqualTo(1);
        assertThat(status.truncatedBytesOnRecovery()).isEqualTo(7L);

        consumer.subscribe(List.of("resilient"));
        List<ConsumedRecord> remaining = consumer.poll(100, 1024 * 1024);
        assertThat(remaining).hasSize(50);
      }
    } finally {
      restarted.destroyForcibly();
      restarted.waitFor(5, TimeUnit.SECONDS);
    }
  }

  private static Process startBrokerProcess(int port, Path dataDir) throws IOException {
    String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    String classpath = System.getProperty("java.class.path");

    ProcessBuilder pb =
        new ProcessBuilder(javaBin, "-cp", classpath, "com.kafkalite.core.BrokerApplication");
    pb.environment().put("BROKER_HOST", "127.0.0.1");
    pb.environment().put("BROKER_PORT", Integer.toString(port));
    pb.environment().put("DATA_DIR", dataDir.toString());
    pb.environment().put("FLUSH_POLICY", "EVERY_WRITE");
    pb.environment().put("BROKER_CONFIG", dataDir.resolve("nonexistent.properties").toString());
    pb.redirectErrorStream(true);
    return pb.start();
  }

  private static void waitForBrokerReady(int port) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      try (Socket s = new Socket()) {
        s.connect(new InetSocketAddress("127.0.0.1", port), 200);
        return;
      } catch (IOException ignored) {
        Thread.sleep(50);
      }
    }
    throw new AssertionError("Child broker process did not start within 10s on port " + port);
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket ss = new ServerSocket(0)) {
      return ss.getLocalPort();
    }
  }
}
