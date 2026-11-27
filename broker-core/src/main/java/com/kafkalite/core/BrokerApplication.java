package com.kafkalite.core;

import com.kafkalite.core.config.BrokerConfig;
import com.kafkalite.core.group.GroupCoordinator;
import com.kafkalite.core.group.OffsetManager;
import com.kafkalite.core.network.BrokerServer;
import com.kafkalite.core.topic.TopicManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
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

    Clock clock = Clock.systemUTC();
    TopicManager topicManager = TopicManager.open(config, Clock.systemUTC());
    OffsetManager offsetManager = OffsetManager.open(config.dataDir());
    GroupCoordinator groupCoordinator =
        new GroupCoordinator(topicManager, config.sessionTimeoutMs(), clock);
    BrokerServer server = new BrokerServer(config, topicManager, groupCoordinator, offsetManager);
    server.start();

    CountDownLatch shutdownLatch = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  log.info("Shutdown signal received; flushing state and stopping broker...");
                  try {
                    server.close();
                  } catch (IOException e) {
                    log.warn("Error closing BrokerServer during shutdown", e);
                  }
                  groupCoordinator.close();
                  try {
                    offsetManager.close();
                  } catch (IOException e) {
                    log.warn("Error closing OffsetManager during shutdown", e);
                  }
                  try {
                    topicManager.close();
                  } catch (IOException e) {
                    log.warn("Error closing TopicManager during shutdown", e);
                  } finally {
                    shutdownLatch.countDown();
                  }
                },
                "broker-shutdown"));
    log.info(
        "Kafkalite broker initialized on {}:{} (dataDir={}, flushPolicy={})",
        config.host(),
        server.boundPort(),
        config.dataDir(),
        config.flushPolicy(),
        topicManager.listTopics().size());

    shutdownLatch.await();
    log.info("Kafkalite broker stopped cleanly.");
  }
}
