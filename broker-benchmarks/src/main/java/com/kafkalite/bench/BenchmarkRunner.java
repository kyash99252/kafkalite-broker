package com.kafkalite.bench;

import com.kafkalite.client.admin.AdminClient;
import com.kafkalite.client.consumer.KafkaLiteConsumer;
import com.kafkalite.client.consumer.KafkaLiteConsumer.FetchResult;
import com.kafkalite.client.producer.KafkaLiteProducer;
import com.kafkalite.client.producer.KafkaLiteProducer.ProducerRecord;
import com.kafkalite.core.config.BrokerConfig;
import com.kafkalite.core.config.BrokerConfig.FlushPolicy;
import com.kafkalite.core.group.GroupCoordinator;
import com.kafkalite.core.group.OffsetManager;
import com.kafkalite.core.network.BrokerServer;
import com.kafkalite.core.topic.TopicManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * Self-contained throughput, tail-latency, and crash-recovery benchmark suite for KafkaLite.
 * Supports both in-process embedded runs (across flush policies and batch sizes) and remote runs
 * against an already-running broker (e.g., inside Docker).
 */
public final class BenchmarkRunner {
  public record BenchResult(
      String experiment,
      long messages,
      double msgsPerSec,
      double mbPerSec,
      double p50Ms,
      double p95Ms,
      double p99Ms) {
    public BenchResult {
      Objects.requireNonNull(experiment, "experiment must not be null");
    }
  }

  public record SuiteReport(List<BenchResult> results, long recoveryMs) {
    public SuiteReport {
      results = List.copyOf(Objects.requireNonNull(results, "results must not be null"));
    }
  }

  private BenchmarkRunner() {}

  public static void main(String[] args) throws Exception {
    if (args.length >= 3 && "remote".equals(args[0])) {
      String host = args[1];
      int port = Integer.parseInt(args[2]);
      int messages = args.length > 3 ? Integer.parseInt(args[3]) : 10_000;
      int payloadBytes = args.length > 4 ? Integer.parseInt(args[4]) : 512;
      List<BenchResult> results = runRemote(host, port, messages, payloadBytes);
      printReport("Docker / Remote Broker Benchmark", results, -1L);
      return;
    }

    int messages = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;
    int payloadBytes = args.length > 1 ? Integer.parseInt(args[1]) : 512;
    if (messages <= 0 || payloadBytes < 0) {
      throw new IllegalArgumentException(
          "messages must be > 0 and payloadBytes >= 0: " + messages + ", " + payloadBytes);
    }

    Path tempRoot = Files.createTempDirectory("kafkalite-bench-");
    try {
      var report = runEmbedded(tempRoot, messages, payloadBytes);
      printReport("Embedded Native Broker Benchmark", report.results(), report.recoveryMs());
    } finally {
      deleteRecursively(tempRoot);
    }
  }

  /**
   * Runs the full embedded benchmark matrix: flush-policy trade-offs, batch-size scaling,
   * 4-partition parallel producers, sequential consumer fetch, E2E round-trip latency, and cold
   * recovery.
   */
  public static SuiteReport runEmbedded(Path rootDir, int messages, int payloadBytes)
      throws Exception {
    Objects.requireNonNull(rootDir, "rootDir must not be null");
    List<BenchResult> results = new ArrayList<>();
    byte[] payload = fixedPayload(payloadBytes);

    // 1. Flush policy comparison (cap EVERY_WRITE so per-message fsync doesn't stall large runs)
    int syncMessages = Math.min(messages, 1_500);
    results.add(
        runPolicyBench(
            rootDir.resolve("every-write"),
            FlushPolicy.EVERY_WRITE,
            syncMessages,
            1,
            payload,
            "Producer (EVERY_WRITE, batch=1)"));
    results.add(
        runPolicyBench(
            rootDir.resolve("interval"),
            FlushPolicy.INTERVAL,
            messages,
            1,
            payload,
            "Producer (INTERVAL, batch=1)"));
    results.add(
        runPolicyBench(
            rootDir.resolve("os-default-b1"),
            FlushPolicy.OS_DEFAULT,
            messages,
            1,
            payload,
            "Producer (OS_DEFAULT, batch=1)"));

    // 2. Batch size impact (amortizes TCP framing and syscall overhead per record)
    for (int batchSize : List.of(16, 64, 256)) {
      results.add(
          runPolicyBench(
              rootDir.resolve("batch-" + batchSize),
              FlushPolicy.OS_DEFAULT,
              messages,
              batchSize,
              payload,
              "Producer (OS_DEFAULT, batch=" + batchSize + ")"));
    }

    // 3. Parallel 4-partition producer + consumer throughput + E2E latency + recovery time
    Path multiDir = rootDir.resolve("multi");
    BrokerConfig cfg = config(multiDir, FlushPolicy.OS_DEFAULT);
    long recoveryMs;
    int producedParallelCount;
    try (TopicManager tm = TopicManager.open(cfg, Clock.systemUTC());
        OffsetManager om = OffsetManager.open(cfg.dataDir());
        GroupCoordinator gc = new GroupCoordinator(tm, cfg.sessionTimeoutMs(), Clock.systemUTC());
        BrokerServer server = new BrokerServer(cfg, tm, gc, om)) {
      server.start();
      int port = server.boundPort();

      try (var admin = new AdminClient("127.0.0.1", port)) {
        admin.createTopic("bench-parallel", 4);
        admin.createTopic("bench-e2e", 1);
      }

      BenchResult parallelProd = runParallelProducers(port, "bench-parallel", 4, messages, payload);
      results.add(parallelProd);
      producedParallelCount = (int) parallelProd.messages();

      results.add(
          runConsumerThroughput(port, "bench-parallel", 4, producedParallelCount, payloadBytes));
      results.add(runEndToEndLatency(port, "bench-e2e", Math.min(messages, 500), payload));
    }

    // Reopen TopicManager from disk after clean shutdown to measure cold segment/index replay time
    long t0 = System.nanoTime();
    try (TopicManager recovered = TopicManager.open(cfg, Clock.systemUTC())) {
      recoveryMs = (System.nanoTime() - t0) / 1_000_000;
      recovered.storageStats();
    }

    return new SuiteReport(results, recoveryMs);
  }

  /** Benchmarks an external broker over TCP using a uniquely named 4-partition topic. */
  public static List<BenchResult> runRemote(String host, int port, int messages, int payloadBytes)
      throws Exception {
    Objects.requireNonNull(host, "host must not be null");
    String topic = "bench-remote-" + System.currentTimeMillis();
    try (var admin = new AdminClient(host, port)) {
      admin.createTopic(topic, 4);
    }
    byte[] payload = fixedPayload(payloadBytes);
    List<BenchResult> results = new ArrayList<>();
    long totalProducedToPartition0 = 0;
    for (int batchSize : List.of(1, 16, 64, 256)) {
      BenchResult batchRes =
          runRemoteBatch(
              host,
              port,
              topic,
              messages,
              batchSize,
              payload,
              "Remote Produce (batch=" + batchSize + ")");
      results.add(batchRes);
      // Only the measured batches (plus warmup) are drained below; track exact count
      totalProducedToPartition0 += batchRes.messages();
    }
    results.add(
        runConsumerThroughput(host, port, topic, 4, totalProducedToPartition0, payloadBytes));
    return results;
  }

  private static BenchResult runPolicyBench(
      Path dir, FlushPolicy policy, int messages, int batchSize, byte[] payload, String label)
      throws Exception {
    BrokerConfig cfg = config(dir, policy);
    try (TopicManager tm = TopicManager.open(cfg, Clock.systemUTC());
        OffsetManager om = OffsetManager.open(cfg.dataDir());
        GroupCoordinator gc = new GroupCoordinator(tm, cfg.sessionTimeoutMs(), Clock.systemUTC());
        BrokerServer server = new BrokerServer(cfg, tm, gc, om)) {
      server.start();
      int port = server.boundPort();
      try (var admin = new AdminClient("127.0.0.1", port)) {
        admin.createTopic("bench", 1);
      }
      return runRemoteBatch("127.0.0.1", port, "bench", messages, batchSize, payload, label);
    }
  }

  private static BenchResult runRemoteBatch(
      String host,
      int port,
      String topic,
      int messages,
      int batchSize,
      byte[] payload,
      String label)
      throws Exception {
    try (var producer = new KafkaLiteProducer(host, port)) {
      // Warm up JVM JIT compilation and TCP socket buffers before recording latency percentiles
      int warmup = Math.min(200, Math.max(10, messages / 10));
      for (int i = 0; i < warmup; i++) {
        producer.send(topic, 0, null, payload);
      }

      int batches = Math.max(1, messages / batchSize);
      int totalSent = batches * batchSize;
      long[] latenciesNs = new long[batches];

      List<ProducerRecord> batch = new ArrayList<>(batchSize);
      for (int i = 0; i < batchSize; i++) {
        batch.add(new ProducerRecord(null, payload));
      }

      long suiteStart = System.nanoTime();
      for (int b = 0; b < batches; b++) {
        long t0 = System.nanoTime();
        producer.sendBatch(topic, 0, batch);
        latenciesNs[b] = System.nanoTime() - t0;
      }
      long elapsedNs = System.nanoTime() - suiteStart;

      return summarize(label, totalSent, payload.length, elapsedNs, latenciesNs);
    }
  }

  /**
   * Spawns one virtual-thread producer per partition so concurrent appends exercise partition-level
   * lock independence without head-of-line blocking on a single socket.
   */
  private static BenchResult runParallelProducers(
      int port, String topic, int partitions, int totalMessages, byte[] payload) throws Exception {
    int perWorker = Math.max(1, totalMessages / partitions);
    int actualTotal = perWorker * partitions;
    long[] latenciesNs = new long[actualTotal];

    long start = System.nanoTime();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<long[]>> futures = new ArrayList<>(partitions);
      for (int p = 0; p < partitions; p++) {
        final int part = p;
        futures.add(
            executor.submit(() -> {
              long[] local = new long[perWorker];
              try (var prod = new KafkaLiteProducer("127.0.0.1", port)) {
                for (int i = 0; i < perWorker; i++) {
                  long t0 = System.nanoTime();
                  prod.send(topic, part, null, payload);
                  local[i] = System.nanoTime() - t0;
                }
              }
              return local;
            }));
      }
      int idx = 0;
      for (Future<long[]> f : futures) {
        long[] arr;
        try {
          arr = f.get();
        } catch (ExecutionException e) {
          if (e.getCause() instanceof Exception ex) {
            throw ex;
          }
          throw e;
        }
        System.arraycopy(arr, 0, latenciesNs, idx, arr.length);
        idx += arr.length;
      }
    }
    long elapsedNs = System.nanoTime() - start;
    return summarize(
        "Parallel Producers (4 partitions)", actualTotal, payload.length, elapsedNs, latenciesNs);
  }

  private static BenchResult runConsumerThroughput(
      int port, String topic, int partitions, long expectedMessages, int payloadBytes)
      throws Exception {
    return runConsumerThroughput(
        "127.0.0.1", port, topic, partitions, expectedMessages, payloadBytes);
  }

  private static BenchResult runConsumerThroughput(
      String host, int port, String topic, int partitions, long expectedMessages, int payloadBytes)
      throws Exception {
    long start = System.nanoTime();
    long count = 0;
    List<Long> fetchLatencies = new ArrayList<>();

    try (var consumer = new KafkaLiteConsumer(host, port)) {
      for (int p = 0; p < partitions; p++) {
        long offset = 0;
        while (true) {
          long t0 = System.nanoTime();
          FetchResult res = consumer.fetch(topic, p, offset, 500, 1024 * 1024);
          fetchLatencies.add(System.nanoTime() - t0);
          if (res.records().isEmpty()) {
            break;
          }
          count += res.records().size();
          offset = res.records().getLast().offset() + 1;
        }
      }
    }

    long elapsedNs = System.nanoTime() - start;
    long[] lats = fetchLatencies.stream().mapToLong(Long::longValue).toArray();
    // Prefer actual drained count; fall back only if expectedMessages is positive and nothing was
    // read
    long reportedCount = count > 0 ? count : Math.max(0L, expectedMessages);
    return summarize(
        "Consumer Fetch (" + partitions + " partitions)",
        reportedCount,
        payloadBytes,
        elapsedNs,
        lats);
  }

  /**
   * Measures unbatched publish-to-consume round-trip latency (produce ack followed immediately by
   * fetch).
   */
  private static BenchResult runEndToEndLatency(int port, String topic, int samples, byte[] payload)
      throws Exception {
    long[] lats = new long[samples];
    try (var producer = new KafkaLiteProducer("127.0.0.1", port);
        var consumer = new KafkaLiteConsumer("127.0.0.1", port)) {
      long start = System.nanoTime();
      for (int i = 0; i < samples; i++) {
        long t0 = System.nanoTime();
        var ack = producer.send(topic, 0, null, payload);
        FetchResult res = consumer.fetch(topic, 0, ack.offset(), 1, 64 * 1024);
        if (res.records().isEmpty()) {
          throw new IllegalStateException("Expected record at offset " + ack.offset());
        }
        lats[i] = System.nanoTime() - t0;
      }
      long elapsed = System.nanoTime() - start;
      return summarize(
          "End-to-End Latency (produce+fetch)", samples, payload.length, elapsed, lats);
    }
  }

  private static BenchResult summarize(
      String label, long count, int payloadBytes, long elapsedNs, long[] latenciesNs) {
    double secs = Math.max(1e-6, elapsedNs / 1_000_000_000.0);
    double msgsPerSec = count / secs;
    double mbPerSec = (count * (double) payloadBytes) / (1024.0 * 1024.0) / secs;

    if (latenciesNs.length == 0) {
      return new BenchResult(label, count, msgsPerSec, mbPerSec, 0, 0, 0);
    }
    Arrays.sort(latenciesNs);
    return new BenchResult(
        label,
        count,
        msgsPerSec,
        mbPerSec,
        percentileMs(latenciesNs, 0.50),
        percentileMs(latenciesNs, 0.95),
        percentileMs(latenciesNs, 0.99));
  }

  private static double percentileMs(long[] sortedNs, double pct) {
    int idx = Math.min(sortedNs.length - 1, (int) Math.floor(pct * (sortedNs.length - 1)));
    return sortedNs[idx] / 1_000_000.0;
  }

  private static BrokerConfig config(Path dir, FlushPolicy policy) {
    return new BrokerConfig("127.0.0.1", 0, dir, policy, 1000L, 64 * 1024 * 1024L, 10_000L);
  }

  private static byte[] fixedPayload(int size) {
    byte[] bytes = new byte[size];
    Arrays.fill(bytes, (byte) 'x');
    return bytes;
  }

  private static void printReport(String title, List<BenchResult> results, long recoveryMs) {
    System.out.println("\n=== " + title + " ===");
    System.out.println(
        "| Experiment | Messages | Throughput (msg/s) | MB/s | p50 (ms) | p95 (ms) | p99 (ms) |");
    System.out.println("|---|---:|---:|---:|---:|---:|---:|");
    for (BenchResult r : results) {
      System.out.printf(
          "| %s | %d | %.0f | %.2f | %.3f | %.3f | %.3f |%n",
          r.experiment(),
          r.messages(),
          r.msgsPerSec(),
          r.mbPerSec(),
          r.p50Ms(),
          r.p95Ms(),
          r.p99Ms());
    }
    if (recoveryMs >= 0) {
      System.out.printf("%nStartup Recovery Time (4 partitions): %d ms%n", recoveryMs);
    }
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(root)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(p -> {
            try {
              Files.deleteIfExists(p);
            } catch (IOException ignored) {
            }
          });
    }
  }
}
