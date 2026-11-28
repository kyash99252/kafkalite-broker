package com.kafkalite.core.lifecycle;

import com.kafkalite.protocol.codec.RecordCodec;
import com.kafkalite.protocol.exception.CorruptRecordException;
import com.kafkalite.protocol.model.Record;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * CLI utility for inspecting {@code .log} segments and verifying CRC32C framing on disk. Walks
 * records sequentially without mutating or truncating the file (read-only diagnostic tool).
 */
public final class LogInspector {
  private LogInspector() {}

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: LogInspector <path-to-segment.log>");
      System.exit(1);
    }
    Path logPath = Path.of(args[0]);
    if (!Files.exists(logPath)) {
      System.err.println("File not found: " + logPath);
      System.exit(1);
    }

    // Positional reads (ch.read(buf, offset)) avoid shifting channel state and tolerate short reads
    try (FileChannel ch = FileChannel.open(logPath, StandardOpenOption.READ)) {
      long fileSize = ch.size();
      long pos = 0;
      int validCount = 0;
      ByteBuffer lenBuf = ByteBuffer.allocate(RecordCodec.LENGTH_PREFIX_BYTES);

      System.out.printf("Inspecting %s (%d bytes)%n", logPath, fileSize);
      while (pos + RecordCodec.LENGTH_PREFIX_BYTES <= fileSize) {
        lenBuf.clear();
        while (lenBuf.hasRemaining() && ch.read(lenBuf, pos + lenBuf.position()) > 0) {}
        if (lenBuf.hasRemaining()) {
          // File shrank mid-inspection (e.g., concurrent log truncation); stop cleanly
          break;
        }
        lenBuf.flip();
        int bodyLen = lenBuf.getInt();

        // Catch both undersized/negative lengths (corrupt/zero-filled tail) and partial trailing
        // frames
        if (bodyLen < RecordCodec.MIN_BODY_BYTES
            || pos + RecordCodec.LENGTH_PREFIX_BYTES + bodyLen > fileSize) {
          System.out.printf("     [TRUNCATED] pos=%d bodyLen=%d%n", pos, bodyLen);
          break;
        }

        // Allocate per-record body buffer rather than reusing one to avoid holding a huge buffer
        // for the rest of the scan after encountering a single large message
        ByteBuffer bodyBuf = ByteBuffer.allocate(bodyLen);
        long readPos = pos + RecordCodec.LENGTH_PREFIX_BYTES;
        while (bodyBuf.hasRemaining()) {
          int n = ch.read(bodyBuf, readPos + bodyBuf.position());
          if (n < 0) {
            break;
          }
        }
        if (bodyBuf.hasRemaining()) {
          System.out.printf(
              "  [TRUNCATED] pos=%d expected=%d read=%d%n", pos, bodyLen, bodyBuf.position());
          break;
        }
        bodyBuf.flip();

        try {
          Record r = RecordCodec.decodeBody(bodyBuf);
          String keyStr = r.key() == null ? "null" : new String(r.key(), StandardCharsets.UTF_8);
          System.out.printf(
              "  [OK] pos=%d offset=%d ts=%d key=%s payloadBytes=%d%n",
              pos, r.offset(), r.timestamp(), keyStr, r.payload().length);
          validCount++;
        } catch (CorruptRecordException e) {
          // Stop at first CRC mismatch; frame boundaries beyond a corrupt record cannot be trusted
          System.out.printf("  [CORRUPT] pos=%d reason=%s%n", pos, e.getMessage());
          break;
        }
        pos += RecordCodec.LENGTH_PREFIX_BYTES + (long) bodyLen;
      }
      // Non-zero trailingBytes points directly to the byte offset where crash recovery would
      // truncate.
      System.out.printf("Summary: validRecords=%d, trailingBytes=%d%n", validCount, fileSize - pos);
    }
  }
}
