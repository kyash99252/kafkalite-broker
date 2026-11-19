package com.kafkalite.protocol.codec;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.kafkalite.protocol.exception.CorruptRecordException;
import com.kafkalite.protocol.model.Record;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RecordCodedTest {

  @Test
  void roundTripWithKeyAndPayload() {
    Record original =
        new Record(
            42L,
            1_700_000_000_000L,
            "order-1".getBytes(StandardCharsets.UTF_8),
            "{\"amount\":100}".getBytes(StandardCharsets.UTF_8));

    ByteBuffer encoded = RecordCodec.encode(original);
    int bodyLen = encoded.getInt();
    assertThat(bodyLen).isEqualTo(encoded.remaining());

    Record decoded = RecordCodec.decodeBody(encoded);
    assertThat(decoded).isEqualTo(original);
  }

  @Test
  void roundTripWithNullKeyAndEmptyPayload() {
    Record original = new Record(0L, 123L, null, new byte[0]);
    ByteBuffer encoded = RecordCodec.encode(original);
    encoded.getInt(); // skip length prefix

    Record decoded = RecordCodec.decodeBody(encoded);
    assertThat(decoded).isEqualTo(original);
    assertThat(decoded.key()).isNull();
    assertThat(decoded.payload()).isEmpty();
  }

  @Test
  void corruptedPayloadFailsCrcCheck() {
    Record original = new Record(1L, 100L, null, "hello".getBytes(StandardCharsets.UTF_8));
    ByteBuffer encoded = RecordCodec.encode(original);
    encoded.getInt(); // skip length prefix

    byte[] body = new byte[encoded.remaining()];
    encoded.get(body);
    body[body.length - 1] ^= 0x01; // flip last bit

    assertThatThrownBy(() -> RecordCodec.decodeBody(ByteBuffer.wrap(body)))
        .isInstanceOf(CorruptRecordException.class)
        .hasMessageContaining("CRC32C mismatch");
  }
}
