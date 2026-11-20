package com.kafkalite.core.topic;

import com.kafkalite.core.config.BrokerConfig;
import com.kafkalite.core.storage.PartitionLog;
import com.kafkalite.protocol.error.ErrorCode;
import com.kafkalite.protocol.exception.BrokerException;
import com.kafkalite.protocol.model.Record;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicManager implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(TopicManager.class);
  private static final Pattern VALID_TOPIC_NAME = Pattern.compile("^[a-zA-Z0-0._-]{1,128}$");
  private static final String META_FILE = "topics.meta";
  private static final String META_TMP_FILE = "topics.meta.tmp";

  public record TopicMetadata(String name, int partitionCount, List<Long> endOffsets) {}

  public record ProduceResult(int partition, Record record) {}

  private static final class TopicState {
    final String name;
    final List<PartitionLog> partitions;
    final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    TopicState(String name, List<PartitionLog> partitions) {
      this.name = name;
      this.partitions = List.copyOf(partitions);
    }
  }

  private final BrokerConfig config;
  private final Clock clock;
  private final Path metadataPath;
  private final Map<String, TopicState> topics = new ConcurrentHashMap<>();
  private final Object metadataLock = new Object();

  private TopicManager(BrokerConfig config, Clock clock) {
    this.config = config;
    this.clock = clock;
    this.metadataPath = config.dataDir().resolve(META_FILE);
  }

  public static TopicManager open(BrokerConfig config, Clock clock) throws IOException {
    Files.createDirectories(config.dataDir());
    TopicManager manager = new TopicManager(config, clock);
    manager.recoverTopicInParallel();
    return manager;
  }

  private void recoverTopicInParallel() throws IOException {
    if (!Files.exists(metadataPath)) {
      log.info("No existing {} found; starting with empty topic catalog", META_FILE);
      return;
    }

    List<String> lines = Files.readAllLines(metadataPath, StandardCharsets.UTF_8);
    List<Map.Entry<String, Integer>> specs = new ArrayList<>();
    for (String rawLine : lines) {
      String line = rawLine.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      int sep = line.indexOf('=');
      if (sep <= 0) {
        throw new IOException("Malformed metadata entry in " + META_FILE + ": " + line);
      }
      String topicName = line.substring(0, sep);
      int partitionCount = Integer.parseInt(line.substring(sep + 1));
      specs.add(Map.entry(topicName, partitionCount));
    }

    if (specs.isEmpty()) {
      return;
    }

    record OpenedPartition(String topic, int partition, PartitionLog log) {}

    List<Future<OpenedPartition>> futures = new ArrayList<>();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (var spec : specs) {
        String topic = spec.getKey();
        int count = spec.getValue();
        for (int p = 0; p < count; p++) {
          final int partitionId = p;
          futures.add(
              executor.submit(() -> {
                Path dir = partitionDir(topic, partitionId);
                PartitionLog pLog =
                    PartitionLog.open(
                        dir,
                        config.segmentMaxBytes(),
                        PartitionLog.DEFAULT_INDEX_INTERVAL_BYTES,
                        config.flushPolicy(),
                        config.flushIntervalMs(),
                        clock);
                return new OpenedPartition(topic, partitionId, pLog);
              }));
        }
      }

      Map<String, PartitionLog[]> assembled = new ConcurrentHashMap<>();
      for (var spec : specs) {
        assembled.put(spec.getKey(), new PartitionLog[spec.getValue()]);
      }

      for (Future<OpenedPartition> f : futures) {
        OpenedPartition opened = f.get();
        assembled.get(opened.topic())[opened.partition()] = opened.log();
      }

      for (var spec : specs) {
        topics.put(
            spec.getKey(),
            new TopicState(spec.getKey(), Arrays.asList(assembled.get(spec.getKey()))));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closeQuietly();
      throw new IOException("Interrupted while recovering topics", e);
    } catch (ExecutionException e) {
      closeQuietly();
      throw new IOException("Failed to recover topic partitions", e.getCause());
    }

    log.info("Recovered {} topic(s) in parallel from {}", topics.size(), config.dataDir());
  }

  public TopicMetadata createTopic(String topicName, int partitionCount) throws IOException {
    validateTopicName(topicName);
    if (partitionCount < 1) {
      throw new BrokerException(
          ErrorCode.INVALID_REQUEST, "Partition count must be >= 1: " + partitionCount);
    }

    synchronized (metadataLock) {
      if (topics.containsKey(topicName)) {
        throw new BrokerException(
            ErrorCode.TOPIC_ALREADY_EXISTS, "Topic already exists: " + topicName);
      }

      List<PartitionLog> logs = new ArrayList<>(partitionCount);
      try {
        for (int p = 0; p < partitionCount; p++) {
          logs.add(
              PartitionLog.open(
                  partitionDir(topicName, p),
                  config.segmentMaxBytes(),
                  PartitionLog.DEFAULT_INDEX_INTERVAL_BYTES,
                  config.flushPolicy(),
                  config.flushIntervalMs(),
                  clock));
        }
        TopicState state = new TopicState(topicName, logs);
        topics.put(topicName, state);
        persistMetadataAtomically();
        log.info("Created topic '{}' with {} partition(s)", topicName, partitionCount);
        return toMetadata(state);
      } catch (IOException e) {
        topics.remove(topicName);
        for (PartitionLog logToClose : logs) {
          try {
            logToClose.close();
          } catch (IOException suppressed) {
            e.addSuppressed(suppressed);
          }
        }
        throw e;
      }
    }
  }

  public ProduceResult publish(String topicName, Integer partition, byte[] key, byte[] payload)
      throws IOException {
    if (payload == null) {
      throw new BrokerException(ErrorCode.INVALID_REQUEST, "Payload must not be null");
    }
    TopicState state = requireTopic(topicName);
    int targetPartition = selectPartition(state, partition, key);
    Record record = state.partitions.get(targetPartition).append(key, payload);
    return new ProduceResult(targetPartition, record);
  }

  public List<Record> fetch(
      String topicName, int partition, long offset, int maxRecords, int maxBytes)
      throws IOException {
    TopicState state = requireTopic(topicName);
    if (partition < 0 || partition >= state.partitions.size()) {
      throw new BrokerException(
          ErrorCode.INVALID_PARTITION,
          "Invalid partition " + partition + " for topic " + topicName);
    }
    if (offset < 0) {
      throw new BrokerException(ErrorCode.OFFSET_OUT_OF_RANGE, "Offset must be >= 0: " + offset);
    }
    return state.partitions.get(partition).read(offset, maxRecords, maxBytes);
  }

  public TopicMetadata describeTopic(String topicName) {
    return toMetadata(requireTopic(topicName));
  }

  public List<TopicMetadata> listTopics() {
    return topics.values().stream()
        .map(TopicManager::toMetadata)
        .sorted((a, b) -> a.name().compareTo(b.name()))
        .toList();
  }

  public boolean topicExists(String topicName) {
    return topicName != null && topics.containsKey(topicName);
  }

  private int selectPartition(TopicState state, Integer explicitPartition, byte[] key) {
    int count = state.partitions.size();
    if (explicitPartition != null) {
      if (explicitPartition < 0 || explicitPartition >= count) {
        throw new BrokerException(
            ErrorCode.INVALID_PARTITION,
            "Invalid partition " + explicitPartition + " for topic " + state.name);
      }
      return explicitPartition;
    }
    if (key != null) {
      return (Arrays.hashCode(key) & 0x7fffffff) % count;
    }
    return (state.roundRobinCounter.getAndIncrement() & 0x7fffffff) % count;
  }

  private TopicState requireTopic(String topicName) {
    if (topicName == null || topicName.isBlank()) {
      throw new BrokerException(ErrorCode.INVALID_REQUEST, "Topic name must not be blank");
    }
    TopicState state = topics.get(topicName);
    if (state == null) {
      throw new BrokerException(ErrorCode.UNKNOWN_TOPIC, "Unknown topic: " + topicName);
    }
    return state;
  }

  private static void validateTopicName(String topicName) {
    if (topicName == null || !VALID_TOPIC_NAME.matcher(topicName).matches()) {
      throw new BrokerException(
          ErrorCode.INVALID_REQUEST,
          "Invalid topic name (allowed: [a-zA-Z0-9.__], 1..128 chars: " + topicName);
    }
  }

  private void persistMetadataAtomically() throws IOException {
    StringBuilder sb = new StringBuilder();
    topics.values().stream()
        .sorted((a, b) -> a.name.compareTo(b.name))
        .forEach(t -> sb.append(t.name).append('=').append(t.partitions.size()).append('\n'));

    Path tmpPath = config.dataDir().resolve(META_TMP_FILE);
    byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
    try (FileChannel ch =
        FileChannel.open(
            tmpPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      while (buf.hasRemaining()) {
        ch.write(buf);
      }
      ch.force(true);
    }
    Files.move(
        tmpPath, metadataPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  private Path partitionDir(String topicName, int partition) {
    return config.dataDir().resolve(topicName).resolve(Integer.toString(partition));
  }

  private static TopicMetadata toMetadata(TopicState state) {
    List<Long> endOffsets = state.partitions.stream().map(PartitionLog::nextOffset).toList();
    return new TopicMetadata(state.name, state.partitions.size(), endOffsets);
  }

  private void closeQuietly() {
    try {
      close();
    } catch (IOException e) {
      log.warn("Error during cleanup close", e);
    }
  }

  @Override
  public void close() throws IOException {
    IOException firstError = null;
    for (TopicState state : topics.values()) {
      for (PartitionLog pLog : state.partitions) {
        try {
          pLog.close();
        } catch (IOException e) {
          if (firstError == null) {
            firstError = e;
          } else {
            firstError.addSuppressed(e);
          }
        }
      }
    }
    if (firstError != null) {
      throw firstError;
    }
  }
}
