package com.kafkalite.core;

import com.kafkalite.core.config.BrokerConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerApplication {
  private static final Logger log = LoggerFactory.getLogger(BrokerApplication.class);

  private BrokerApplication() {}

  public static void main(String[] args) throws Exception {
    Path configPath =
        Path.of(System.getenv().getOrDefault("BROKER_CONFIG", "config/broker.properties"));
    BrokerConfig config = BrokerConfig.load(configPath);
    Files.createDirectories(config.dataDir());

    CountDownLatch shutdownLatch = new CountDownLatch(1);
    ServerSocket placeholderSocket = new ServerSocket();
    placeholderSocket.setReuseAddress(true);
    placeholderSocket.bind(new InetSocketAddress(config.host(), config.port()));

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Shutdown signal received; flushing state and stopping broker...");
                  try {
                    placeholderSocket.close();
                  } catch (IOException e) {
                    log.warn("Error closing broker socket during shutdown", e);
                  } finally {
                    shutdownLatch.countDown();
                  }
                },
                "broker-shutdown"));
    log.info(
        "Kafkalite broker initialized on {}:{} (dataDir={}, flushPolicy={})",
        config.host(),
        config.port(),
        config.dataDir(),
        config.flushPolicy());

    shutdownLatch.await();
    log.info("Kafkalite broker stopped cleanly.");
  }
}
