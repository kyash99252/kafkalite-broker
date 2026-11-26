package com.kafkalite.protocol.codec;

import com.kafkalite.protocol.error.ErrorCode;
import com.kafkalite.protocol.model.Record;
import com.kafkalite.protocol.model.Request;
import com.kafkalite.protocol.model.Response;
import com.kafkalite.protocol.model.TopicPartition;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Encodes and decodes request and response frames exchanged over the wire. */
public final class WireCodec {
  public static final int MAX_FRAME_BYTES = 32 * 1024 * 1024; // 32 MiB
  private static final int MIN_FRAME_BYTES = 5; // 4B correlationId + 1B opCode

  private static final byte OP_CREATE_TOPIC = 1;
  private static final byte OP_DESCRIBE_TOPIC = 2;
  private static final byte OP_LIST_TOPICS = 3;
  private static final byte OP_PRODUCE = 4;
  private static final byte OP_FETCH = 5;
  private static final byte OP_JOIN_GROUP = 6;
  private static final byte OP_HEARTBEAT = 7;
  private static final byte OP_LEAVE_GROUP = 8;
  private static final byte OP_COMMIT_OFFSET = 9;
  private static final byte OP_FETCH_OFFSET = 10;

  private static final byte OP_ERROR = 100;
  private static final byte OP_TOPIC_CREATED = 101;
  private static final byte OP_TOPIC_DESCRIBED = 102;
  private static final byte OP_TOPICS_LISTED = 103;
  private static final byte OP_PRODUCED = 104;
  private static final byte OP_FETCHED = 105;
  private static final byte OP_GROUP_JOINED = 106;
  private static final byte OP_HEARTBEAT_ACK = 107;
  private static final byte OP_GROUP_LEFT = 108;
  private static final byte OP_OFFSET_COMMITTED = 109;
  private static final byte OP_OFFSET_FETCHED = 110;

  /** Associates a decoded message with the request it belongs to. */
  public record Envelope<T>(int correlationId, T payload) {}

  /** Serializes a request using the opcode and field order defined by the wire protocol. */
  public static void writeRequest(DataOutputStream out, int correlationId, Request request)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
    DataOutputStream frame = new DataOutputStream(baos);
    frame.writeInt(correlationId);

    switch (request) {
      case Request.CreateTopic(var topic, var partitionCount) -> {
        frame.writeByte(OP_CREATE_TOPIC);
        writeString(frame, topic);
        frame.writeInt(partitionCount);
      }
      case Request.DescribeTopic(var topic) -> {
        frame.writeByte(OP_DESCRIBE_TOPIC);
        writeString(frame, topic);
      }
      case Request.ListTopics() -> frame.writeByte(OP_LIST_TOPICS);
      case Request.Produce(var topic, var partition, var entries) -> {
        frame.writeByte(OP_PRODUCE);
        writeString(frame, topic);
        frame.writeInt(partition == null ? -1 : partition);
        frame.writeInt(entries.size());
        for (Request.ProduceEntry entry : entries) {
          writeBytes(frame, entry.key());
          writeBytes(frame, entry.payload());
        }
      }
      case Request.Fetch(var topic, var partition, var offset, var maxRecords, var maxBytes) -> {
        frame.writeByte(OP_FETCH);
        writeString(frame, topic);
        frame.writeInt(partition);
        frame.writeLong(offset);
        frame.writeInt(maxRecords);
        frame.writeInt(maxBytes);
      }
      case Request.JoinGroup(var groupId, var memberId, var topics) -> {
        frame.writeByte(OP_JOIN_GROUP);
        writeString(frame, groupId);
        writeString(frame, memberId);
        frame.writeInt(topics.size());
        for (String t : topics) {
          writeString(frame, t);
        }
      }
      case Request.Heartbeat(var groupId, var memberId, var generationId) -> {
        frame.writeByte(OP_HEARTBEAT);
        writeString(frame, groupId);
        writeString(frame, memberId);
        frame.writeInt(generationId);
      }
      case Request.LeaveGroup(var groupId, var memberId) -> {
        frame.writeByte(OP_LEAVE_GROUP);
        writeString(frame, groupId);
        writeString(frame, memberId);
      }
      case Request.CommitOffset(var groupId, var tp, var offset) -> {
        frame.writeByte(OP_COMMIT_OFFSET);
        writeString(frame, groupId);
        writeString(frame, tp.topic());
        frame.writeInt(tp.partition());
        frame.writeLong(offset);
      }
      case Request.FetchOffset(var groupId, var tp) -> {
        frame.writeByte(OP_FETCH_OFFSET);
        writeString(frame, groupId);
        writeString(frame, tp.topic());
        frame.writeInt(tp.partition());
      }
    }
    sendFrame(out, baos.toByteArray());
  }

  /** Serializes a response while preserving the originating request's correlation ID. */
  public static void writeResponse(DataOutputStream out, int correlationId, Response response)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
    DataOutputStream frame = new DataOutputStream(baos);
    frame.writeInt(correlationId);

    switch (response) {
      case Response.ErrorResponse(var code, var message) -> {
        frame.writeByte(OP_ERROR);
        frame.writeShort(code.code());
        writeString(frame, message);
      }
      case Response.TopicCreated(var topic) -> {
        frame.writeByte(OP_TOPIC_CREATED);
        writeTopicInfo(frame, topic);
      }
      case Response.TopicDescribed(var topic) -> {
        frame.writeByte(OP_TOPIC_DESCRIBED);
        writeTopicInfo(frame, topic);
      }
      case Response.TopicsListed(var topics) -> {
        frame.writeByte(OP_TOPICS_LISTED);
        frame.writeInt(topics.size());
        for (Response.TopicInfo info : topics) {
          writeTopicInfo(frame, info);
        }
      }
      case Response.Produced(var topic, var offsets) -> {
        frame.writeByte(OP_PRODUCED);
        writeString(frame, topic);
        frame.writeInt(offsets.size());
        for (Response.ProducedOffset off : offsets) {
          frame.writeInt(off.partition());
          frame.writeLong(off.offset());
          frame.writeLong(off.timestamp());
        }
      }
      case Response.Fetched(var topic, var partition, var hwm, var records) -> {
        frame.writeByte(OP_FETCHED);
        writeString(frame, topic);
        frame.writeInt(partition);
        frame.writeLong(hwm);
        frame.writeInt(records.size());
        for (Record r : records) {
          frame.writeLong(r.offset());
          frame.writeLong(r.timestamp());
          writeBytes(frame, r.key());
          writeBytes(frame, r.payload());
        }
      }
      case Response.GroupJoined(var groupId, var memberId, var genId, var assignment) -> {
        frame.writeByte(OP_GROUP_JOINED);
        writeString(frame, groupId);
        writeString(frame, memberId);
        frame.writeInt(genId);
        writeTopicPartitions(frame, assignment);
      }
      case Response.HeartbeatAck(var genId, var assignment) -> {
        frame.writeByte(OP_HEARTBEAT_ACK);
        frame.writeInt(genId);
        writeTopicPartitions(frame, assignment);
      }
      case Response.GroupLeft(var groupId, var memberId) -> {
        frame.writeByte(OP_GROUP_LEFT);
        writeString(frame, groupId);
        writeString(frame, memberId);
      }
      case Response.OffsetCommitted(var groupId, var tp, var offset) -> {
        frame.writeByte(OP_OFFSET_COMMITTED);
        writeString(frame, groupId);
        writeString(frame, tp.topic());
        frame.writeInt(tp.partition());
        frame.writeLong(offset);
      }
      case Response.OffsetFetched(var groupId, var tp, var offset) -> {
        frame.writeByte(OP_OFFSET_FETCHED);
        writeString(frame, groupId);
        writeString(frame, tp.topic());
        frame.writeInt(tp.partition());
        frame.writeLong(offset);
      }
    }
    sendFrame(out, baos.toByteArray());
  }

  /** Returns null only when the stream ends between frames. */
  public static Envelope<Request> readRequest(DataInputStream in) throws IOException {
    ByteBuffer buf = readFrameBuffer(in);
    if (buf == null) {
      return null;
    }
    int correlationId = buf.getInt();
    byte op = buf.get();
    Request req =
        switch (op) {
          case OP_CREATE_TOPIC -> new Request.CreateTopic(readString(buf), buf.getInt());
          case OP_DESCRIBE_TOPIC -> new Request.DescribeTopic(readString(buf));
          case OP_LIST_TOPICS -> new Request.ListTopics();
          case OP_PRODUCE -> readProduce(buf);
          case OP_FETCH ->
              new Request.Fetch(
                  readString(buf), buf.getInt(), buf.getLong(), buf.getInt(), buf.getInt());
          case OP_JOIN_GROUP -> readJoinGroup(buf);
          case OP_HEARTBEAT ->
              new Request.Heartbeat(readString(buf), readString(buf), buf.getInt());
          case OP_LEAVE_GROUP -> new Request.LeaveGroup(readString(buf), readString(buf));
          case OP_COMMIT_OFFSET ->
              new Request.CommitOffset(
                  readString(buf),
                  new TopicPartition(readString(buf), buf.getInt()),
                  buf.getLong());
          case OP_FETCH_OFFSET ->
              new Request.FetchOffset(
                  readString(buf), new TopicPartition(readString(buf), buf.getInt()));
          default -> throw new IllegalArgumentException("Unknown request opcode: " + op);
        };
    ensureFullyConsumed(buf);
    return new Envelope<>(correlationId, req);
  }

  /** Reads a response frame; EOF is an error because a response was expected. */
  public static Envelope<Response> readResponse(DataInputStream in) throws IOException {
    ByteBuffer buf = readFrameBuffer(in);
    if (buf == null) {
      throw new EOFException("Connection close while waiting for response");
    }
    int correlationId = buf.getInt();
    byte op = buf.get();
    Response resp =
        switch (op) {
          case OP_ERROR ->
              new Response.ErrorResponse(decodeErrorCode(buf.getShort()), readString(buf));
          case OP_TOPIC_CREATED -> new Response.TopicCreated(readTopicInfo(buf));
          case OP_TOPIC_DESCRIBED -> new Response.TopicDescribed(readTopicInfo(buf));
          case OP_TOPICS_LISTED -> readTopicsListed(buf);
          case OP_PRODUCED -> readProduced(buf);
          case OP_FETCHED -> readFetched(buf);
          case OP_GROUP_JOINED ->
              new Response.GroupJoined(
                  readString(buf), readString(buf), buf.getInt(), readTopicPartitions(buf));
          case OP_HEARTBEAT_ACK ->
              new Response.HeartbeatAck(buf.getInt(), readTopicPartitions(buf));
          case OP_GROUP_LEFT -> new Response.GroupLeft(readString(buf), readString(buf));
          case OP_OFFSET_COMMITTED ->
              new Response.OffsetCommitted(
                  readString(buf),
                  new TopicPartition(readString(buf), buf.getInt()),
                  buf.getLong());
          case OP_OFFSET_FETCHED ->
              new Response.OffsetFetched(
                  readString(buf),
                  new TopicPartition(readString(buf), buf.getInt()),
                  buf.getLong());
          default -> throw new IllegalArgumentException("Unknown response opcode: " + op);
        };
    ensureFullyConsumed(buf);
    return new Envelope<>(correlationId, resp);
  }

  // A negative partition represents an unassigned partition in produce requests.
  private static Request.Produce readProduce(ByteBuffer buf) {
    String topic = readString(buf);
    int rawPartition = buf.getInt();
    Integer partition = rawPartition < 0 ? null : rawPartition;
    int count = readValidCount(buf);
    List<Request.ProduceEntry> entries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      byte[] key = readBytes(buf);
      byte[] payload = readBytes(buf);
      if (payload == null) {
        throw new IllegalArgumentException("Produce entry payload must not be null");
      }
      entries.add(new Request.ProduceEntry(key, payload));
    }
    return new Request.Produce(topic, partition, entries);
  }

  private static void writeTopicPartitions(DataOutputStream out, List<TopicPartition> list)
      throws IOException {
    out.writeInt(list.size());
    for (TopicPartition tp : list) {
      writeString(out, tp.topic());
      out.writeInt(tp.partition());
    }
  }

  private static List<TopicPartition> readTopicPartitions(ByteBuffer buf) {
    int count = readValidCount(buf);
    List<TopicPartition> list = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      list.add(new TopicPartition(readString(buf), buf.getInt()));
    }
    return list;
  }

  private static Response.TopicsListed readTopicsListed(ByteBuffer buf) {
    int count = readValidCount(buf);
    List<Response.TopicInfo> list = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      list.add(readTopicInfo(buf));
    }
    return new Response.TopicsListed(list);
  }

  private static Response.Produced readProduced(ByteBuffer buf) {
    String topic = readString(buf);
    int count = readValidCount(buf);
    List<Response.ProducedOffset> offsets = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      offsets.add(new Response.ProducedOffset(buf.getInt(), buf.getLong(), buf.getLong()));
    }
    return new Response.Produced(topic, offsets);
  }

  private static Request.JoinGroup readJoinGroup(ByteBuffer buf) {
    String groupId = readString(buf);
    String memberId = readString(buf);
    int count = readValidCount(buf);
    List<String> topics = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      topics.add(readString(buf));
    }
    return new Request.JoinGroup(groupId, memberId, topics);
  }

  private static Response.Fetched readFetched(ByteBuffer buf) {
    String topic = readString(buf);
    int partition = buf.getInt();
    long hwm = buf.getLong();
    int count = readValidCount(buf);
    List<Record> records = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      long offset = buf.getLong();
      long ts = buf.getLong();
      byte[] key = readBytes(buf);
      byte[] payload = readBytes(buf);
      records.add(new Record(offset, ts, key, payload == null ? new byte[0] : payload));
    }
    return new Response.Fetched(topic, partition, hwm, records);
  }

  private static void writeTopicInfo(DataOutputStream out, Response.TopicInfo info)
      throws IOException {
    writeString(out, info.name());
    out.writeInt(info.partitionCount());
    out.writeInt(info.endOffsets().size());
    for (long endOffset : info.endOffsets()) {
      out.writeLong(endOffset);
    }
  }

  private static Response.TopicInfo readTopicInfo(ByteBuffer buf) {
    String name = readString(buf);
    int partitionCount = buf.getInt();
    int count = readValidCount(buf);
    List<Long> endOffsets = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      endOffsets.add(buf.getLong());
    }
    return new Response.TopicInfo(name, partitionCount, endOffsets);
  }

  // The length prefix is excluded from the frame size and covers the payload only.
  private static void sendFrame(DataOutputStream out, byte[] payload) throws IOException {
    if (payload.length > MAX_FRAME_BYTES) {
      throw new IllegalArgumentException("Frame exceeds max size: " + payload.length);
    }
    out.writeInt(payload.length);
    out.write(payload);
    out.flush();
  }

  // Read the header separately so a clean connection close can be distinguished
  // from a partially received frame.
  private static ByteBuffer readFrameBuffer(DataInputStream in) throws IOException {
    int b1 = in.read();
    if (b1 < 0) {
      return null; // Clean EOF between frames
    }
    int b2 = in.read();
    int b3 = in.read();
    int b4 = in.read();
    if ((b2 | b3 | b4) < 0) {
      throw new EOFException("Truncated frame length header");
    }
    int frameLen = (b1 << 24) | (b2 << 16) | (b3 << 8) | b4;
    if (frameLen < MIN_FRAME_BYTES || frameLen > MAX_FRAME_BYTES) {
      throw new IllegalArgumentException("Invalid frame length: " + frameLen);
    }
    byte[] bytes = new byte[frameLen];
    in.readFully(bytes);
    return ByteBuffer.wrap(bytes);
  }

  // Length -1 is reserved for null; all non-null strings are encoded as UTF-8.
  private static void writeString(DataOutputStream out, String value) throws IOException {
    if (value == null) {
      out.writeInt(-1);
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static String readString(ByteBuffer buf) {
    byte[] bytes = readBytes(buf);
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
  }

  // Byte arrays use the same length-prefix convention as strings.
  private static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
    if (bytes == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static byte[] readBytes(ByteBuffer buf) {
    int len = buf.getInt();
    if (len == -1) {
      return null;
    }
    if (len < 0 || len > buf.remaining()) {
      throw new IllegalArgumentException("Invalid byte array length: " + len);
    }
    byte[] bytes = new byte[len];
    buf.get(bytes);
    return bytes;
  }

  // Counts are bounded by the remaining frame size before allocating collections.
  private static int readValidCount(ByteBuffer buf) {
    int count = buf.getInt();
    if (count < 0 || count > buf.remaining()) {
      throw new IllegalArgumentException("Invalid item count: " + count);
    }
    return count;
  }

  // Reject extra data so malformed or mismatched protocol versions are not ignored.
  private static void ensureFullyConsumed(ByteBuffer buf) {
    if (buf.hasRemaining()) {
      throw new IllegalArgumentException("Trailing unread bytes in frame: " + buf.remaining());
    }
  }

  // Unknown codes are mapped to a stable fallback for forward compatibility.
  private static ErrorCode decodeErrorCode(short raw) {
    for (ErrorCode c : ErrorCode.values()) {
      if (c.code() == raw) {
        return c;
      }
    }
    return ErrorCode.UNKNOWN_ERROR;
  }
}
