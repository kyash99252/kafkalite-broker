package com.kafkalite.protocol.model;

import java.util.List;
import java.util.Objects;

public sealed interface Request
    permits Request.CreateTopic,
        Request.DescribeTopic,
        Request.ListTopics,
        Request.Produce,
        Request.Fetch,
        Request.JoinGroup,
        Request.Heartbeat,
        Request.LeaveGroup,
        Request.CommitOffset,
        Request.FetchOffset {
  record CreateTopic(String topic, int partitionCount) implements Request {}

  record DescribeTopic(String topic) implements Request {}

  record ListTopics() implements Request {}

  record ProduceEntry(byte[] key, byte[] payload) {
    public ProduceEntry {
      Objects.requireNonNull(payload, "payload must not be null");
    }
  }

  record Produce(String topic, Integer partition, List<ProduceEntry> entries) implements Request {
    public Produce {
      Objects.requireNonNull(entries, "entries must not be null");
    }
  }

  record Fetch(String topic, int partition, long offset, int maxRecords, int maxBytes)
      implements Request {}

  record JoinGroup(String groupId, String memberId, List<String> topics) implements Request {
    public JoinGroup {
      Objects.requireNonNull(topics, "topics must not be null");
    }
  }

  record Heartbeat(String groupId, String memberId, int generationId) implements Request {}

  record LeaveGroup(String groupId, String memberId) implements Request {}

  record CommitOffset(String groupId, TopicPartition topicPartition, long offset)
      implements Request {
    public CommitOffset {
      Objects.requireNonNull(topicPartition, "topicPartition must not be null");
    }
  }

  record FetchOffset(String groupId, TopicPartition topicPartition) implements Request {
    public FetchOffset {
      Objects.requireNonNull(topicPartition, "topicPartition must not be null");
    }
  }
}
