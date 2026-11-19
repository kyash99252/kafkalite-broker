package com.kafkalite.core.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkalite.core.config.BrokerConfig.FlushPolicy;
import com.kafkalite.protocol.model.Record;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionLogTest {
  private static final Clock FIXED_CLOCK =
      Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

  @TempDir Path tempDir;

  @Test
  void appendReadAndRollSegmentsAcrossRestart() throws Exception {
    Path partDir = tempDir.resolve("orders-0");
    try (PartitionLog log =
        PartitionLog.open(partDir, 128, 32, FlushPolicy.EVERY_WRITE, 0, FIXED_CLOCK)) {
      for (int i = 0; i < 10; i++) {
        Record r =
            log.append(
                ("k-" + i).getBytes(StandardCharsets.UTF_8),
                ("payload-data-" + i).getBytes(StandardCharsets.UTF_8));
        assertThat(r.offset()).isEqualTo(i);
      }
      assertThat(log.segmentCount()).isGreaterThan(1);
    }

    // Re-open from disk and verify full recovery & sparse index lookup
    try (PartitionLog recovered =
        PartitionLog.open(partDir, 128, 32, FlushPolicy.EVERY_WRITE, 0, FIXED_CLOCK)) {
      assertThat(recovered.nextOffset()).isEqualTo(10L);
      List<Record> slice = recovered.read(4L, 4, 64 * 1024);
      assertThat(slice).extracting(Record::offset).containsExactly(4L, 5L, 6L, 7L);
    }
  }

  @Test
  void truncatesIncompleteTrailingWriteOnRecovery() throws Exception {
    Path partDir = tempDir.resolve("crash-0");
    try (PartitionLog log =
        PartitionLog.open(partDir, 4096, 64, FlushPolicy.EVERY_WRITE, 0, FIXED_CLOCK)) {
      log.append(null, "valid-0".getBytes(StandardCharsets.UTF_8));
      log.append(null, "valid-1".getBytes(StandardCharsets.UTF_8));
    }

    // Simulate partial write (5 garbage trailing bytes)
    Path segFile = partDir.resolve("00000000000000000000.log");
    long validSize = Files.size(segFile);
    try (FileChannel ch = FileChannel.open(segFile, StandardOpenOption.WRITE)) {
      ch.position(validSize);
      ch.write(ByteBuffer.wrap(new byte[] {0, 0, 0, 50, 99}));
    }

    try (PartitionLog recovered =
        PartitionLog.open(partDir, 4096, 64, FlushPolicy.EVERY_WRITE, 0, FIXED_CLOCK)) {
      assertThat(recovered.nextOffset()).isEqualTo(2L);
      assertThat(Files.size(segFile)).isEqualTo(validSize);
      assertThat(recovered.read(0L, 10, 64 * 1024))
          .extracting(r -> new String(r.payload(), StandardCharsets.UTF_8))
          .containsExactly("valid-0", "valid-1");
    }
  }

  @Test
  void supportsConcurrentWritersAndParallelReaders() throws Exception {
    Path partDir = tempDir.resolve("concurrent-0");
    try (PartitionLog log =
            PartitionLog.open(partDir, 4096, 128, FlushPolicy.OS_DEFAULT, 0, FIXED_CLOCK);
        var executor = Executors.newVirtualThreadPerTaskExecutor()) {

      List<Future<?>> writers = new ArrayList<>();
      for (int w = 0; w < 8; w++) {
        writers.add(
            executor.submit(() -> {
              for (int i = 0; i < 50; i++) {
                log.append(null, "msg".getBytes(StandardCharsets.UTF_8));
              }
              return null;
            }));
      }
      for (Future<?> f : writers) {
        f.get();
      }

      assertThat(log.nextOffset()).isEqualTo(400L);

      // Parallel readers reading concurrently
      List<Future<List<Record>>> readers = new ArrayList<>();
      for (int r = 0; r < 8; r++) {
        readers.add(executor.submit(() -> log.read(0L, 400, 1024 * 1024)));
      }
      for (Future<List<Record>> f : readers) {
        assertThat(f.get()).hasSize(400);
      }
    }
  }
}
