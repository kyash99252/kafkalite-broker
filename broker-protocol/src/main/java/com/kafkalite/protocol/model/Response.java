package com.kafkalite.protocol.model;

import com.kafkalite.protocol.error.ErrorCode;
import java.util.List;

public sealed interface Response
    permits Response.ErrorResponse,
        Response.TopicCreated,
        Response.TopicDescribed,
        Response.TopicsListed,
        Response.Produced,
        Response.Fetched {
  record ErrorResponse(ErrorCode errorCode, String message) implements Response {}

  record TopicInfo(String name, int partitionCount, List<Long> endOffsets) {}

  record TopicCreated(TopicInfo topic) implements Response {}

  record TopicDescribed(TopicInfo topic) implements Response {}

  record TopicsListed(List<TopicInfo> topics) implements Response {}

  record ProducedOffset(int partition, long offset, long timestamp) {}

  record Produced(String topic, List<ProducedOffset> offsets) implements Response {}

  record Fetched(String topic, int partition, long highWatermark, List<Record> records)
      implements Response {}
}
