package com.kafkalite.client.producer;

import com.kafkalite.client.transport.BrokerConnection;
import com.kafkalite.protocol.model.Request;
import com.kafkalite.protocol.model.Response;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Provides synchronous and asynchronous message publishing to a broker. */
public final class KafkaLiteProducer implements Closeable {
  public record ProducerRecord(byte[] key, byte[] payload) {}

  public record ProduceAck(int partition, long offset, long timestamp) {}

  private final BrokerConnection connection;

  // Handles asynchronous produce requests using virtual threads.
  private final ExecutorService asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();

  public KafkaLiteProducer(String host, int port) throws IOException {
    this.connection = BrokerConnection.connect(host, port);
  }

  // Publishes a record using the broker's default partition selection.
  public ProduceAck send(String topic, byte[] key, byte[] payload) throws IOException {
    return send(topic, null, key, payload);
  }

  // Publishes a record to the specified partition.
  public ProduceAck send(String topic, Integer partition, byte[] key, byte[] payload)
      throws IOException {
    List<ProduceAck> acks = sendBatch(topic, partition, List.of(new ProducerRecord(key, payload)));
    return acks.getFirst();
  }

  /** Publishes multiple records and returns their assigned offsets. */
  public List<ProduceAck> sendBatch(String topic, Integer partition, List<ProducerRecord> records)
      throws IOException {
    List<Request.ProduceEntry> entries =
        records.stream().map(r -> new Request.ProduceEntry(r.key(), r.payload())).toList();

    Response.Produced produced =
        (Response.Produced) connection.send(new Request.Produce(topic, partition, entries));

    // Map broker-assigned offsets to producer acknowledgements.
    return produced.offsets().stream()
        .map(o -> new ProduceAck(o.partition(), o.offset(), o.timestamp()))
        .toList();
  }

  /** Submits a produce request without blocking the calling thread. */
  public CompletableFuture<ProduceAck> sendAsync(
      String topic, Integer partition, byte[] key, byte[] payload) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return send(topic, partition, key, payload);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        asyncExecutor);
  }

  /** Shuts down the executor and closes the broker connection. */
  @Override
  public void close() throws IOException {
    asyncExecutor.close();
    connection.close();
  }
}
