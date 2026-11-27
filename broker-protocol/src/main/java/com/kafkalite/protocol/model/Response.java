package com.kafkalite.protocol.model;

import com.kafkalite.protocol.error.ErrorCode;
import java.util.List;

public sealed interface Response
    permits Response.ErrorResponse,
        Response.TopicCreated,
        Response.TopicDescribed,
        Response.TopicsListed,
        Response.Produced,
        Response.Fetched,
        Response.GroupJoined,
        Response.HeartbeatAck,
        Response.GroupLeft,
        Response.OffsetCommitted,
        Response.OffsetFetched,
        Response.BrokerStatus {
  record ErrorResponse(ErrorCode errorCode, String message) implements Response {}

  record TopicInfo(String name, int partitionCount, List<Long> endOffsets) {}

  record TopicCreated(TopicInfo topic) implements Response {}

  record TopicDescribed(TopicInfo topic) implements Response {}

  record TopicsListed(List<TopicInfo> topics) implements Response {}

  record ProducedOffset(int partition, long offset, long timestamp) {}

  record Produced(String topic, List<ProducedOffset> offsets) implements Response {}

  record Fetched(String topic, int partition, long highWatermark, List<Record> records)
      implements Response {}

  record GroupJoined(
      String groupId, String memberId, int generationId, List<TopicPartition> assignment)
      implements Response {}

  record HeartbeatAck(int generationId, List<TopicPartition> assignment) implements Response {}

  record GroupLeft(String groupId, String memberId) implements Response {}

  record OffsetCommitted(String groupId, TopicPartition topicPartition, long offset)
      implements Response {}

  /** committedOffset is -1 when no offset has been committed yet. */
  record OffsetFetched(String groupId, TopicPartition topicPartition, long committedOffset)
      implements Response {}

  record ParitionLag(
          String topic, int partition, long highWatermark, long committedOffset, long lag
  ) {}

  record GroupStatusInfo(
          String groupId, int generationId, int memberCount, List<ParitionLag> partitions
  ) {}

  record BrokerStatus(
          long uptimeMs,
          int topicCount,
          int totalPartitions,
          long totalSegments,
          long totalStorageBytes,
          long recoveryDurationMs,
          long truncatedBytesOnRecovery,
          List<GroupStatusInfo> groups
  ) implements Response {}
}
