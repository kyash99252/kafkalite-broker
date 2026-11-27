package com.kafkalite.core.storage;

import com.kafkalite.core.config.BrokerConfig.FlushPolicy;
import com.kafkalite.protocol.model.Record;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PartitionLog implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(PartitionLog.class);
  public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;

  private final Path partitionDir;
  private final long segmentMaxBytes;
  private final int indexIntervalBytes;
  private final FlushPolicy flushPolicy;
  private final Clock clock;

  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final NavigableMap<Long, LogSegment> segments = new TreeMap<>();
  private final ScheduledExecutorService flushScheduler;

  private LogSegment activeSegment;
  private long nextOffset;

  private PartitionLog(
      Path partitionDir,
      long segmentMaxBytes,
      int indexIntervalBytes,
      FlushPolicy flushPolicy,
      long flushIntervalMs,
      Clock clock)
      throws IOException {
    this.partitionDir = partitionDir;
    this.segmentMaxBytes = segmentMaxBytes;
    this.indexIntervalBytes = indexIntervalBytes;
    this.flushPolicy = flushPolicy;
    this.clock = clock;

    loadExistingSegments();

    if (flushPolicy == FlushPolicy.INTERVAL) {
      this.flushScheduler =
          Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "partition-flush-" + partitionDir.getFileName());
            t.setDaemon(true);
            return t;
          });
      this.flushScheduler.scheduleAtFixedRate(
          this::safeFlush, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    } else {
      this.flushScheduler = null;
    }
  }

  public static PartitionLog open(
      Path partitionDir,
      long segmentMaxBytes,
      int indexIntervalBytes,
      FlushPolicy flushPolicy,
      long flushIntervalMs,
      Clock clock)
      throws IOException {
    return new PartitionLog(
        partitionDir, segmentMaxBytes, indexIntervalBytes, flushPolicy, flushIntervalMs, clock);
  }

  private void loadExistingSegments() throws IOException {
    Files.createDirectories(partitionDir);
    List<Long> baseOffsets = new ArrayList<>();
    try (Stream<Path> files = Files.list(partitionDir)) {
      files
          .filter(p -> p.getFileName().toString().endsWith(".log"))
          .forEach(p -> {
            String name = p.getFileName().toString();
            String numPart = name.substring(0, name.length() - 4);
            baseOffsets.add(Long.parseLong(numPart));
          });
    }
    baseOffsets.sort(Comparator.naturalOrder());

    if (baseOffsets.isEmpty()) {
      LogSegment intial = LogSegment.open(partitionDir, 0L, indexIntervalBytes);
      segments.put(0L, intial);
      activeSegment = intial;
      nextOffset = 0L;
      return;
    }

    for (long base : baseOffsets) {
      LogSegment segment = LogSegment.open(partitionDir, base, indexIntervalBytes);
      segments.put(base, segment);
      activeSegment = segment;
      nextOffset = Math.max(nextOffset, segment.nextOffset());
    }

    log.info(
        "Recovered partition {} with {} segment(s), nextOffset={}",
        partitionDir,
        segments.size(),
        nextOffset);
  }

  public Record append(byte[] key, byte[] payload) throws IOException {
    rwLock.writeLock().lock();
    try {
      if (activeSegment.sizeBytes() >= segmentMaxBytes) {
        rollSegment();
      }
      Record record = new Record(nextOffset, clock.millis(), key, payload);
      activeSegment.append(record);
      nextOffset++;

      if (flushPolicy == FlushPolicy.EVERY_WRITE) {
        activeSegment.flush(true);
      }
      return record;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /** Concurrent reads share the readLock and use positional FileChannel reads. */
  public List<Record> read(long startOffset, int maxRecords, int maxBytes) throws IOException {
    if (startOffset < 0) {
      throw new IllegalArgumentException("startOffset must be non-negative: " + startOffset);
    }

    rwLock.readLock().lock();
    try {
      if (startOffset >= nextOffset || maxRecords <= 0) {
        return List.of();
      }

      List<Record> collected = new ArrayList<>();
      int remainingBytes = maxBytes;

      Map.Entry<Long, LogSegment> entry = segments.floorEntry(startOffset);
      if (entry == null) {
        entry = segments.firstEntry();
      }

      long cursorOffset = startOffset;
      while (entry != null && collected.size() < maxRecords && remainingBytes > 0) {
        LogSegment seg = entry.getValue();
        List<Record> batch =
            seg.readFrom(cursorOffset, maxRecords - collected.size(), remainingBytes);
        for (Record r : batch) {
          collected.add(r);
          cursorOffset = r.offset() + 1;
          int approxBytes =
              Record.class == r.getClass()
                  ? (r.key() == null ? 0 : r.key().length) + r.payload().length
                  : 0;
          remainingBytes = Math.max(0, remainingBytes - approxBytes);
        }
        entry = segments.higherEntry(entry.getKey());
      }
      return collected;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public void flush() throws IOException {
    rwLock.readLock().lock();
    try {
      if (activeSegment != null) {
        activeSegment.flush(true);
      }
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public long nextOffset() {
    rwLock.readLock().lock();
    try {
      return nextOffset;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public int segmentCount() {
    rwLock.readLock().lock();
    try {
      return segments.size();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  private void rollSegment() throws IOException {
    activeSegment.flush(true);
    LogSegment next = LogSegment.open(partitionDir, nextOffset, indexIntervalBytes);
    segments.put(nextOffset, next);
    activeSegment = next;
  }

  private void safeFlush() {
    try {
      flush();
    } catch (IOException e) {
      log.warn("Periodic flush failed for {}", partitionDir, e);
    }
  }

  public long totalSizeBytes() {
    rwLock.readLock().lock();
    try {
      return segments.values().stream().mapToLong(LogSegment::sizeBytes).sum();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public long truncatedBytesOnRecovery() {
    rwLock.readLock().lock();
    try {
      return segments.values().stream().mapToLong(LogSegment::truncatedBytesOnRecovery).sum();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (flushScheduler != null) {
      flushScheduler.shutdown();
      try {
        if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          flushScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        flushScheduler.shutdownNow();
      }
    }

    rwLock.writeLock().lock();
    try {
      for (LogSegment segment : segments.values()) {
        segment.close();
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }
}
