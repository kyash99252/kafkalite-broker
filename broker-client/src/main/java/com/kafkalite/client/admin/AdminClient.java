package com.kafkalite.client.admin;

import com.kafkalite.client.transport.BrokerConnection;
import com.kafkalite.protocol.model.Request;
import com.kafkalite.protocol.model.Response;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Synchronous admin client for managing and inspecting KafkaLite topics. Shares a single {@link
 * BrokerConnection}; callers should treat instances as single-threaded unless the underlying
 * transport guarantees multiplexing.
 */
public final class AdminClient implements Closeable {

  private final BrokerConnection connection;

  public AdminClient(String host, int port) throws IOException {
    this.connection = BrokerConnection.connect(host, port);
  }

  /** Creates a new topic with the requested partition count. */
  public Response.TopicInfo createTopic(String topic, int partitionCount) throws IOException {
    validateTopicName(topic);
    if (partitionCount < 1) {
      throw new IllegalArgumentException("partitionCount must be >= 1: " + partitionCount);
    }

    // Type pattern avoids hardcoding record component arity while still catching protocol
    // mismatches
    return switch (connection.send(new Request.CreateTopic(topic, partitionCount))) {
      case Response.TopicCreated created -> created.topic();
      case Response other ->
          throw new IOException("Unexpected broker response for CreateTopic: " + other);
    };
  }

  /** Fetches metadata (partition count, offsets, etc.) for an existing topic. */
  public Response.TopicInfo describeTopic(String topic) throws IOException {
    validateTopicName(topic);

    return switch (connection.send(new Request.DescribeTopic(topic))) {
      case Response.TopicDescribed described -> described.topic();
      case Response other ->
          throw new IOException("Unexpected broker response for DescribeTopic: " + other);
    };
  }

  /** Returns metadata for all topics currently registered on the broker. */
  public List<Response.TopicInfo> listTopics() throws IOException {
    return switch (connection.send(new Request.ListTopics())) {
      // Defensive copy so callers cannot mutate internal broker response state
      case Response.TopicsListed listed -> List.copyOf(listed.topics());
      case Response other ->
          throw new IOException("Unexpected broker response for ListTopics: " + other);
    };
  }

  private static void validateTopicName(String topic) {
    Objects.requireNonNull(topic, "topic must not be null");
    if (topic.isBlank()) {
      throw new IllegalArgumentException("topic must not be blank");
    }
  }

  @Override
  public void close() throws IOException {
    connection.close();
  }
}
