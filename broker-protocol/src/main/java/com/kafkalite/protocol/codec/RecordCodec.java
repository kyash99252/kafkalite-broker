package com.kafkalite.protocol.codec;

import com.kafkalite.protocol.exception.CorruptRecordException;
import com.kafkalite.protocol.model.Record;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public class RecordCodec {
    public static final int LENGTH_PREFIX_BYTES = 4;
    public static final int MIN_BODY_BYTES = 4 + 8 + 8 + 4 + 4;     // crc + offset + ts + keylen + payloadlen
    public static final int DEFAULT_MAX_RECORD_BYTES = 16 * 1024 * 1024;    //  16 MiB

    private RecordCodec() {
    }

    public static ByteBuffer encode(Record record) {
        int keyLen = record.key() == null ? -1 : record.key().length;
        int keyBytes = Math.max(0, keyLen);
        int payloadLen = record.payload().length;
        int bodyLen = MIN_BODY_BYTES + keyBytes + payloadLen;

        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_PREFIX_BYTES + bodyLen);
        buffer.putInt(bodyLen);

        int crcPos = buffer.position();
        buffer.position(crcPos + 4);        // Reserve space for CRC32C

        buffer.putLong(record.offset());
        buffer.putLong(record.timestamp());
        buffer.putInt(keyLen);
        if (keyLen > 0) {
            buffer.put(record.key());
        }
        buffer.putInt(payloadLen);
        if (payloadLen > 0) {
            buffer.put(record.payload());
        }

        int crcOffset = crcPos + 4;
        int crcValue = computeCrc32c(buffer.array(), crcOffset, bodyLen - 4);
        buffer.putInt(crcPos, crcValue);

        buffer.flip();
        return buffer;
    }

    public static Record decodeBody(ByteBuffer bodyBuffer) {
        if (bodyBuffer.remaining() < MIN_BODY_BYTES) {
            throw new CorruptRecordException(
                    "Record body too short: " + bodyBuffer.remaining() + " bytes"
            );
        }

        int startPos = bodyBuffer.position();
        int bodyLen = bodyBuffer.remaining();
        int expectedCrc = bodyBuffer.getInt();

        ByteBuffer crcSlice = bodyBuffer.slice();
        int actualCrc = computeCrc32c(crcSlice);
        if (expectedCrc != actualCrc) {
            throw new CorruptRecordException(
                    "CRC32 mismatch: expected=" + expectedCrc + ", actual=" + actualCrc
            );
        }

        long offset = bodyBuffer.getLong();
        long timestamp = bodyBuffer.getLong();

        int keyLen = bodyBuffer.getInt();
        if (keyLen < -1 || keyLen > bodyBuffer.remaining() - 4) {
            throw new CorruptRecordException("Invalid key length: " + keyLen);
        }

        byte[] key = null;
        if (keyLen >= 0) {
            key = new byte[keyLen];
            bodyBuffer.get(key);
        }

        int payloadLen = bodyBuffer.getInt();
        if (payloadLen < 0 || payloadLen != bodyBuffer.remaining()) {
            throw new CorruptRecordException(
                    "Invalid payload length: " + payloadLen + ", remaining=" + bodyBuffer.remaining()
            );
        }

        byte[] payload = new byte[payloadLen];
        bodyBuffer.get(payload);

        if (bodyBuffer.position() - startPos != bodyLen) {
            throw new CorruptRecordException("Unexpected trailing bytes in record body");
        }

        return new Record(offset, timestamp, key, payload);
    }

    private static int computeCrc32c(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    private static int computeCrc32c(ByteBuffer slice) {
        CRC32C crc = new CRC32C();
        crc.update(slice);
        return (int) crc.getValue();
    }
}
