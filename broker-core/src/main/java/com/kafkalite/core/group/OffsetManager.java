package com.kafkalite.core.group;

import com.kafkalite.protocol.error.ErrorCode;
import com.kafkalite.protocol.exception.BrokerException;
import com.kafkalite.protocol.model.TopicPartition;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable, thread-safe consumer offset store backed by a single flat metadata file. Reads are
 * lock-free via {@link ConcurrentHashMap}; writes serialize through {@link #persistLock} so on-disk
 * snapshots never race or reorder out of sequence with in-memory state.
 */
public final class OffsetManager implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(OffsetManager.class);

  // Restrict delimiters ('|', '=', newlines) out of groupId so the flat-file format stays
  // unambiguous
  private static final Pattern VALID_GROUP_ID = Pattern.compile("[a-zA-Z0-9._-]{1,128}$");
  private static final String OFFSETS_FILE = "offsets.meta";
  private static final String OFFSETS_TMP_FILE = "offsets.meta.tmp";

  public record OffsetKey(String groupId, String topic, int partition) {}

  private final Path dataDir;
  private final Path offsetsPath;
  private final Map<OffsetKey, Long> committedOffsets = new ConcurrentHashMap<>();

  // Guards the compound (in-memory update + atomic disk flush) critical section.
  // ReentrantLock instead of synchronized avoids carrier-thread pinning on Java 21+/25 virtual
  // threads
  private final ReentrantLock persistLock = new ReentrantLock();

  private OffsetManager(Path dataDir) {
    this.dataDir = dataDir;
    this.offsetsPath = dataDir.resolve(OFFSETS_FILE);
  }

  /** Initializes the offset directory and replays any existing {@code offsets.meta} snapshot. */
  public static OffsetManager open(Path dataDir) throws IOException {
    Objects.requireNonNull(dataDir, "dataDir must not be null");
    Files.createDirectories(dataDir);
    OffsetManager manager = new OffsetManager(dataDir);
    manager.loadFromDisk();
    return manager;
  }

  private void loadFromDisk() throws IOException {
    if (!Files.exists(offsetsPath)) {
      return;
    }
    for (String raw : Files.readAllLines(offsetsPath, StandardCharsets.UTF_8)) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      int eq = line.indexOf('=');
      if (eq <= 0) {
        throw new IOException("Malformed entry in " + OFFSETS_FILE + ": " + line);
      }
      // Note: '|' is safe as a delimiter only if topic names also disallow '|' (same rule as
      // groupId).
      String[] parts = line.substring(0, eq).split("\\|");
      if (parts.length != 3) {
        throw new IOException("Malformed offset key in " + OFFSETS_FILE + ": " + line);
      }
      try {
        int partition = Integer.parseInt(parts[2]);
        long offset = Long.parseLong(line.substring(eq + 1));
        committedOffsets.put(new OffsetKey(parts[0], parts[1], partition), offset);
      } catch (NumberFormatException e) {
        // Wrap parse failures as IOException so startup treats corrupt numeric fields consistently
        throw new IOException("Corrupt numeric value in " + OFFSETS_FILE + ": " + line, e);
      }
    }
    log.info("Recovered {} committed offset(s) from {}", committedOffsets.size(), offsetsPath);
  }

  /** Atomically updates and durably flushes the committed offset for a group/partition. */
  public void commitOffset(String groupId, TopicPartition tp, long offset) throws IOException {
    validateGroupId(groupId);
    Objects.requireNonNull(tp, "tp must not be null");
    if (offset < 0) {
      throw new BrokerException(
          ErrorCode.OFFSET_OUT_OF_RANGE, "Committed offset must be >= 0: " + offset);
    }
    var key = new OffsetKey(groupId, tp.topic(), tp.partition());
    persistLock.lock();
    try {
      committedOffsets.put(key, offset);
      persistAtomically();
    } finally {
      persistLock.unlock();
    }
  }

  /**
   * Returns the last committed offset, or {@code -1L} if this group has never committed for {@code
   * tp}.
   */
  public long fetchOffset(String groupId, TopicPartition tp) {
    validateGroupId(groupId);
    Objects.requireNonNull(tp, "tp must not be null");
    return committedOffsets.getOrDefault(new OffsetKey(groupId, tp.topic(), tp.partition()), -1L);
  }

  /**
   * Returns an immutable point-in-time snapshot of all committed partition offsets for {@code
   * groupId}.
   */
  public Map<TopicPartition, Long> groupOffsets(String groupId) {
    validateGroupId(groupId);
    Map<TopicPartition, Long> result = new HashMap<>();
    committedOffsets.forEach((key, offset) -> {
      if (key.groupId().equals(groupId)) {
        result.put(new TopicPartition(key.topic(), key.partition()), offset);
      }
    });
    // Unmodifiable copy prevents callers from mutating the returned snapshot
    return Map.copyOf(result);
  }

  public List<String> knownGroupIds() {
    return committedOffsets.keySet().stream().map(OffsetKey::groupId).distinct().sorted().toList();
  }

  static void validateGroupId(String groupId) {
    if (groupId == null || !VALID_GROUP_ID.matcher(groupId).matches()) {
      throw new BrokerException(
          ErrorCode.INVALID_REQUEST,
          "Invalid consumer group ID (allowed: [a-zA-Z0-9._-], 1..128 chars): " + groupId);
    }
  }

  private void persistAtomically() throws IOException {
    var sb = new StringBuilder();
    // Sort deterministically so diffs and crash-recovery debugging on offsets.meta are predictable
    committedOffsets.entrySet().stream()
        .sorted(
            Comparator.comparing((Map.Entry<OffsetKey, Long> e) -> e.getKey().groupId())
                .thenComparing(e -> e.getKey().topic())
                .thenComparingInt(e -> e.getKey().partition()))
        .forEach(e -> {
          OffsetKey k = e.getKey();
          sb.append(k.groupId())
              .append('|')
              .append(k.topic())
              .append('|')
              .append(k.partition())
              .append('=')
              .append(e.getValue())
              .append('\n');
        });
    // Write-temp -> fsync -> atomic rename guarantees readers never see a half-written file on
    // crash
    Path tmpPath = dataDir.resolve(OFFSETS_TMP_FILE);
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
        tmpPath, offsetsPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public void close() throws IOException {
    persistLock.lock();
    try {
      // Re-flush on shutdown in case an earlier commit updated memory before a transient I/O error
      if (!committedOffsets.isEmpty()) {
        persistAtomically();
      }
    } finally {
      persistLock.unlock();
    }
  }
}
