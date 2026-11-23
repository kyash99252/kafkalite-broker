package com.kafkalite.core.network;

import com.kafkalite.core.config.BrokerConfig;
import com.kafkalite.core.topic.TopicManager;
import com.kafkalite.core.topic.TopicManager.ProduceResult;
import com.kafkalite.core.topic.TopicManager.TopicMetadata;
import com.kafkalite.protocol.codec.WireCodec;
import com.kafkalite.protocol.codec.WireCodec.Envelope;
import com.kafkalite.protocol.error.ErrorCode;
import com.kafkalite.protocol.exception.BrokerException;
import com.kafkalite.protocol.exception.CorruptRecordException;
import com.kafkalite.protocol.model.Record;
import com.kafkalite.protocol.model.Request;
import com.kafkalite.protocol.model.Response;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles client connections and dispatches incoming requests to the broker. */
public final class BrokerServer implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(BrokerServer.class);

  private final BrokerConfig config;
  private final TopicManager topicManager;

  // Each client connection is handled independently using a virtual thread.
  private final ExecutorService clientExecutor = Executors.newVirtualThreadPerTaskExecutor();

  private volatile boolean running;
  private ServerSocket serverSocket;
  private Thread acceptThread;

  public BrokerServer(BrokerConfig config, TopicManager topicManager) {
    this.config = config;
    this.topicManager = topicManager;
  }

  /** Binds the server socket and starts accepting client connections. */
  public void start() throws IOException {
    serverSocket = new ServerSocket();
    serverSocket.setReuseAddress(true);
    serverSocket.bind(new InetSocketAddress(config.host(), config.port()));
    running = true;

    acceptThread = Thread.ofVirtual().name("broker-accept").start(this::acceptLoop);
    log.info("BrokerServer listening on {}:{}", config.host(), boundPort());
  }

  public int boundPort() {
    return serverSocket == null ? -1 : serverSocket.getLocalPort();
  }

  // Accepts incoming connections and delegates each one to the client executor.
  private void acceptLoop() {
    while (running) {
      try {
        Socket client = serverSocket.accept();
        clientExecutor.submit(() -> handleClient(client));
      } catch (SocketException e) {
        if (running) {
          log.warn("Accept loop socket error", e);
        }
      } catch (IOException e) {
        if (running) {
          log.error("Unexpected I/O error accepting client", e);
        }
      }
    }
  }

  // Maintains the request-response loop for a single client connection.
  private void handleClient(Socket socket) {
    try (socket;
        DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream out =
            new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
      socket.setTcpNoDelay(true);

      while (running) {
        Envelope<Request> envelope;
        try {
          envelope = WireCodec.readRequest(in);
        } catch (EOFException e) {
          log.debug("Client disconnected mid frame: {}", socket.getRemoteSocketAddress());
          break;
        } catch (IllegalArgumentException e) {
          // Invalid frames cannot be safely processed, so the connection is closed.
          log.warn("Malformed request frame from {}", socket.getRemoteSocketAddress(), e);
          WireCodec.writeResponse(
              out, -1, new Response.ErrorResponse(ErrorCode.INVALID_REQUEST, e.getMessage()));
          break;
        }

        if (envelope == null) {
          // Clean EOF indicates that the client has closed the connection.
          break;
        }

        Response response = dispatchSafely(envelope.payload());
        WireCodec.writeResponse(out, envelope.correlationId(), response);
      }
    } catch (IOException e) {
      if (running) {
        log.debug("Client connection closed: {}", socket.getRemoteSocketAddress(), e);
      }
    }
  }

  // Converts request processing failures into protocol-level error responses.
  private Response dispatchSafely(Request request) {
    try {
      return dispatch(request);
    } catch (BrokerException e) {
      return new Response.ErrorResponse(e.errorCode(), e.getMessage());
    } catch (CorruptRecordException e) {
      log.error("Corrupt records encountered during fetch", e);
      return new Response.ErrorResponse(
          ErrorCode.CORRUPT_RECORD, "Stored record failed integrity verification");
    } catch (IllegalArgumentException e) {
      return new Response.ErrorResponse(ErrorCode.INVALID_REQUEST, e.getMessage());
    } catch (IOException e) {
      log.error("Storage I/O error while handling {}", request.getClass().getSimpleName(), e);
      return new Response.ErrorResponse(ErrorCode.STORAGE_FAILURE, "Internal storage failure");
    } catch (RuntimeException e) {
      log.error("Unexpected error while handling {}", request.getClass().getSimpleName(), e);
      return new Response.ErrorResponse(ErrorCode.UNKNOWN_ERROR, "Internal broker error");
    }
  }

  // Routes each request to the corresponding TopicManager operation.
  private Response dispatch(Request request) throws IOException {
    return switch (request) {
      case Request.CreateTopic(var topic, var partitionCount) -> {
        TopicMetadata meta = topicManager.createTopic(topic, partitionCount);
        yield new Response.TopicCreated(toTopicInfo(meta));
      }
      case Request.DescribeTopic(var topic) -> {
        TopicMetadata meta = topicManager.describeTopic(topic);
        yield new Response.TopicDescribed(toTopicInfo(meta));
      }
      case Request.ListTopics() -> {
        List<Response.TopicInfo> infos =
            topicManager.listTopics().stream().map(BrokerServer::toTopicInfo).toList();
        yield new Response.TopicsListed(infos);
      }
      case Request.Produce(var topic, var partition, var entries) -> {
        if (entries.isEmpty()) {
          throw new BrokerException(ErrorCode.INVALID_REQUEST, "Produce batch must not be empty");
        }

        // Publish each entry and collect its assigned partition and offset.
        List<Response.ProducedOffset> produced = new ArrayList<>(entries.size());
        for (Request.ProduceEntry entry : entries) {
          ProduceResult res = topicManager.publish(topic, partition, entry.key(), entry.payload());
          produced.add(
              new Response.ProducedOffset(
                  res.partition(), res.record().offset(), res.record().timestamp()));
        }

        yield new Response.Produced(topic, produced);
      }
      case Request.Fetch(var topic, var partition, var offset, var maxRecords, var maxBytes) -> {
        TopicMetadata meta = topicManager.describeTopic(topic);
        List<Record> records = topicManager.fetch(topic, partition, offset, maxRecords, maxBytes);

        // Include the partition's current end offset in the fetch response.
        long highWatermark = meta.endOffsets().get(partition);
        yield new Response.Fetched(topic, partition, highWatermark, records);
      }
    };
  }

  // Converts internal topic metadata into the protocol response representation.
  private static Response.TopicInfo toTopicInfo(TopicMetadata meta) {
    return new Response.TopicInfo(meta.name(), meta.partitionCount(), meta.endOffsets());
  }

  /** Stops accepting requests and shuts down the client executor. */
  @Override
  public void close() throws IOException {
    running = false;

    // Closing the server socket unblocks any pending accept call.
    if (serverSocket != null) {
      serverSocket.close();
    }

    clientExecutor.shutdown();
    try {
      if (!clientExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        clientExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      clientExecutor.shutdownNow();
    }

    if (acceptThread != null) {
      try {
        acceptThread.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
