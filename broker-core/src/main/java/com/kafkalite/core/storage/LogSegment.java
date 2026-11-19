package com.kafkalite.core.storage;

import com.kafkalite.protocol.codec.RecordCodec;
import com.kafkalite.protocol.exception.CorruptRecordException;
import com.kafkalite.protocol.model.Record;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogSegment implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(LogSegment.class);
  private static final int INDEX_ENTRY_BYTES = 12; // 4B relativeOffset + 8B position

  private record IndexEntry(int relativeOffset, long position) {}

  private final long baseOffset;
  private final Path logPath;
  private final Path indexPath;
  private final FileChannel logChannel;
  private final FileChannel indexChannel;
  private final int indexIntervalBytes;
  private final List<IndexEntry> index = new ArrayList<>();

  private volatile long currentSizeBytes;
  private long bytesSinceLastIndex;
  private long nextOffset;

  private LogSegment(
      long baseOffset,
      Path logPath,
      Path indexPath,
      FileChannel logChannel,
      FileChannel indexChannel,
      int indexIntervalBytes) {
    this.baseOffset = baseOffset;
    this.logPath = logPath;
    this.indexPath = indexPath;
    this.logChannel = logChannel;
    this.indexChannel = indexChannel;
    this.indexIntervalBytes = indexIntervalBytes;
    this.nextOffset = baseOffset;
  }

  public static LogSegment open(Path dir, long baseOffset, int indexIntervalBytes)
      throws IOException {
    Files.createDirectories(dir);
    String prefix = "%020d".formatted(baseOffset);
    Path logPath = dir.resolve(prefix + ".log");
    Path indexPath = dir.resolve(prefix + ".index");

    FileChannel logCh =
        FileChannel.open(
            logPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

    FileChannel idxCh =
        FileChannel.open(
            indexPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);

    LogSegment segment =
        new LogSegment(baseOffset, logPath, indexPath, logCh, idxCh, indexIntervalBytes);
    segment.recover();
    return segment;
  }

  private void recover() throws IOException {
    long fileSize = logChannel.size();
    long pos = 0;
    long bytesSinceIndex = indexIntervalBytes; // Force index entry on first valid record
    ByteBuffer lenBuf = ByteBuffer.allocate(RecordCodec.LENGTH_PREFIX_BYTES);

    index.clear();
    indexChannel.truncate(0);
    indexChannel.position(0);

    while (pos + RecordCodec.LENGTH_PREFIX_BYTES <= fileSize) {
      lenBuf.clear();
      readFully(logChannel, lenBuf, pos);
      lenBuf.flip();
      int bodyLen = lenBuf.getInt();

      if (bodyLen < RecordCodec.MIN_BODY_BYTES
          || bodyLen > RecordCodec.DEFAULT_MAX_RECORD_BYTES
          || pos + RecordCodec.LENGTH_PREFIX_BYTES + bodyLen > fileSize) {
        log.warn(
            "Truncating incomplete trailing record in {} at byte {} (bodyLen={})",
            logPath.getFileName(),
            pos,
            bodyLen);
        break;
      }

      ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen);
      readFully(logChannel, bodyBuf, pos + RecordCodec.LENGTH_PREFIX_BYTES);
      bodyBuf.flip();

      Record record;
      try {
        record = RecordCodec.decodeBody(bodyBuf);
      } catch (CorruptRecordException e) {
        log.warn(
            "Truncating corrupted trailing record in {} at byte {}:{}",
            logPath.getFileName(),
            pos,
            e.getMessage());
        break;
      }

      if (bytesSinceIndex >= indexIntervalBytes) {
        appendIndexEntry((int) (record.offset() - baseOffset), pos);
        bytesSinceIndex = 0;
      }

      int frameBytes = RecordCodec.LENGTH_PREFIX_BYTES + bodyLen;
      pos += frameBytes;
      bytesSinceIndex += frameBytes;
      nextOffset = record.offset() + 1;
    }

    if (pos < fileSize) {
      log.warn(
          "Recovered {} up to {} bytes; truncated {} trailing bytes",
          logPath.getFileName(),
          pos,
          fileSize - pos);
      logChannel.truncate(pos);
    }

    logChannel.position(pos);
    currentSizeBytes = pos;
    bytesSinceLastIndex = bytesSinceIndex;
  }

  public long append(Record record) throws IOException {
    ByteBuffer encoded = RecordCodec.encode(record);
    int frameBytes = encoded.remaining();
    long writePos = currentSizeBytes;

    while (encoded.hasRemaining()) {
      logChannel.write(encoded);
    }

    if (bytesSinceLastIndex >= indexIntervalBytes || index.isEmpty()) {
      appendIndexEntry((int) (record.offset() - baseOffset), writePos);
      bytesSinceLastIndex = 0;
    } else {
      bytesSinceLastIndex += frameBytes;
    }

    currentSizeBytes = writePos + frameBytes;
    nextOffset = record.offset() + 1;
    return writePos;
  }

  /** Thread-safe positional read (supports concurrent readers without locking file cursor). */
  public List<Record> readFrom(long startOffset, int maxRecords, int maxBytes) throws IOException {
    long limitPos = currentSizeBytes;
    if (limitPos == 0 || startOffset >= nextOffset) {
      return List.of();
    }

    long scanPos = lookupFloorPosition(startOffset);
    List<Record> results = new ArrayList<>();
    int accumulatedBytes = 0;
    ByteBuffer lenBuf = ByteBuffer.allocate(RecordCodec.LENGTH_PREFIX_BYTES);

    while (scanPos + RecordCodec.LENGTH_PREFIX_BYTES <= limitPos && results.size() < maxRecords) {
      lenBuf.clear();
      readFully(logChannel, lenBuf, scanPos);
      lenBuf.flip();
      int bodyLen = lenBuf.getInt();

      if (bodyLen < RecordCodec.MIN_BODY_BYTES
          || scanPos + RecordCodec.LENGTH_PREFIX_BYTES + bodyLen > limitPos) {
        break;
      }

      ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen);
      readFully(logChannel, bodyBuf, scanPos + RecordCodec.LENGTH_PREFIX_BYTES);
      bodyBuf.flip();

      Record record = RecordCodec.decodeBody(bodyBuf);
      int frameBytes = RecordCodec.LENGTH_PREFIX_BYTES + bodyLen;
      scanPos += frameBytes;

      if (record.offset() >= startOffset) {
        if (!results.isEmpty() && accumulatedBytes + frameBytes > maxBytes) {
          break;
        }
        results.add(record);
        accumulatedBytes += frameBytes;
      }
    }
    return results;
  }

  public void flush(boolean includeMetadata) throws IOException {
    logChannel.force(includeMetadata);
    indexChannel.force(includeMetadata);
  }

  public long baseOffset() {
    return baseOffset;
  }

  public long nextOffset() {
    return nextOffset;
  }

  public long sizeBytes() {
    return currentSizeBytes;
  }

  private long lookupFloorPosition(long targetOffset) {
    if (index.isEmpty() || targetOffset <= baseOffset) {
      return 0L;
    }
    long relTarget = targetOffset - baseOffset;
    int low = 0;
    int high = index.size() - 1;
    long floorPos = 0L;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      IndexEntry entry = index.get(mid);
      if (entry.relativeOffset() <= relTarget) {
        floorPos = entry.position();
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return floorPos;
  }

  private void appendIndexEntry(int relativeOffset, long position) throws IOException {
    IndexEntry entry = new IndexEntry(relativeOffset, position);
    index.add(entry);

    ByteBuffer buf = ByteBuffer.allocate(INDEX_ENTRY_BYTES);
    buf.putInt(relativeOffset);
    buf.putLong(position);
    buf.flip();
    while (buf.hasRemaining()) {
      indexChannel.write(buf);
    }
  }

  private static void readFully(FileChannel channel, ByteBuffer dst, long position)
      throws IOException {
    long currentPos = position;
    while (dst.hasRemaining()) {
      int read = channel.read(dst, currentPos);
      if (read == -1) {
        break;
      }
      currentPos += read;
    }
  }

  @Override
  public void close() throws IOException {
    IOException firstError = null;
    try {
      flush(true);
      logChannel.close();
    } catch (IOException e) {
      firstError = e;
    }

    try {
      indexChannel.close();
    } catch (IOException e) {
      if (firstError == null) {
        firstError = e;
      } else {
        firstError.addSuppressed(e);
      }
    }

    if (firstError != null) {
      throw firstError;
    }
  }
}
