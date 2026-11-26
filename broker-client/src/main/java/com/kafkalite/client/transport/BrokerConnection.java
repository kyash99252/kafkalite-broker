package com.kafkalite.client.transport;

import com.kafkalite.protocol.codec.WireCodec;
import com.kafkalite.protocol.codec.WireCodec.Envelope;
import com.kafkalite.protocol.exception.BrokerException;
import com.kafkalite.protocol.model.Request;
import com.kafkalite.protocol.model.Response;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/** Manages the client connection to a broker. */
public final class BrokerConnection implements Closeable {
  private final Socket socket;
  private final DataInputStream in;
  private final DataOutputStream out;

  // Ensures requests and responses are processed sequentially on this connection.
  private final ReentrantLock ioLock = new ReentrantLock();

  // Used to match responses with their corresponding requests.
  private final AtomicInteger correlationSeq = new AtomicInteger(1);

  private BrokerConnection(Socket socket) throws IOException {
    this.socket = socket;
    this.socket.setTcpNoDelay(true);
    this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    this.out =
        new DataOutputStream(
            new BufferedOutputStream(new BufferedOutputStream(socket.getOutputStream())));
  }

  /** Establishes a connection using the configured timeout. */
  public static BrokerConnection connect(String host, int port, int connectTimeoutMs)
      throws IOException {
    Socket socket = new Socket();
    socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
    return new BrokerConnection(socket);
  }

  // Uses the default connection timeout.
  public static BrokerConnection connect(String host, int port) throws IOException {
    return connect(host, port, 5000);
  }

  /** Sends a request and waits for its corresponding broker response. */
  public Response send(Request request) throws IOException {
    ioLock.lock();
    try {
      int corrId = correlationSeq.getAndIncrement();
      WireCodec.writeRequest(out, corrId, request);

      Envelope<Response> envelope = WireCodec.readResponse(in);
      if (envelope.correlationId() != corrId) {
        throw new IOException(
            "Correlation ID mismatch: expected=" + corrId + ", got=" + envelope.correlationId());
      }

      Response response = envelope.payload();
      if (response instanceof Response.ErrorResponse(var code, var msg)) {
        throw new BrokerException(code, msg);
      }
      return response;
    } finally {
      ioLock.unlock();
    }
  }

  /** Closes the broker connection and releases its socket resources. */
  @Override
  public void close() throws IOException {
    ioLock.lock();
    try {
      socket.close();
    } finally {
      ioLock.unlock();
    }
  }
}
