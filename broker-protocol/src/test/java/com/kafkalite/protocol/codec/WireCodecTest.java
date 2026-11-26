package com.kafkalite.protocol.codec;

import com.kafkalite.protocol.codec.WireCodec.Envelope;
import com.kafkalite.protocol.error.ErrorCode;
import com.kafkalite.protocol.model.Record;
import com.kafkalite.protocol.model.Request;
import com.kafkalite.protocol.model.Response;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WireCodecTest {

    @Test
    void roundTripProduceAndFetchFrames() throws Exception {
        Request req =
                new Request.Produce(
                        "orders",
                        null,
                        List.of(
                                new Request.ProduceEntry(
                                        "k1".getBytes(StandardCharsets.UTF_8),
                                        "v1".getBytes(StandardCharsets.UTF_8))));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        WireCodec.writeRequest(new DataOutputStream(baos), 77, req);

        Envelope<Request> decodedReq =
                WireCodec.readRequest(
                        new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        assertThat(decodedReq).isNotNull();
        assertThat(decodedReq.correlationId()).isEqualTo(77);
        assertThat(decodedReq.payload()).isInstanceOf(Request.Produce.class);

        Response resp =
                new Response.Fetched(
                        "orders",
                        0,
                        1L,
                        List.of(
                                new Record(
                                        0L,
                                        1000L,
                                        "k1".getBytes(StandardCharsets.UTF_8),
                                        "v1".getBytes(StandardCharsets.UTF_8))));
        baos.reset();
        WireCodec.writeResponse(new DataOutputStream(baos), 77, resp);

        Envelope<Response> decodedResp =
                WireCodec.readResponse(
                        new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        assertThat(decodedResp.correlationId()).isEqualTo(77);
        assertThat(decodedResp.payload()).isEqualTo(resp);
    }

    @Test
    void roundTripErrorResponse() throws Exception {
        Response err = new Response.ErrorResponse(ErrorCode.UNKNOWN_TOPIC, "Unknown topic: x");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        WireCodec.writeResponse(new DataOutputStream(baos), 9, err);

        Envelope<Response> decoded =
                WireCodec.readResponse(
                        new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        assertThat(decoded.payload()).isEqualTo(err);
    }

    @Test
    void rejectsOversizedFrameHeader() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new DataOutputStream(baos).writeInt(WireCodec.MAX_FRAME_BYTES + 1);

        assertThatThrownBy(
                () ->
                        WireCodec.readRequest(
                                new DataInputStream(
                                        new ByteArrayInputStream(baos.toByteArray()))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid frame length");
    }
}