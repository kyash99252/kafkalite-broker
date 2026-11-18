package com.kafkalite.core.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public record BrokerConfig(
    String host,
    int port,
    Path dataDir,
    FlushPolicy flushPolicy,
    long flushIntervalMs,
    long segmentMaxBytes,
    long sessionTimeoutMs) {

  public enum FlushPolicy {
    EVERY_WRITE,
    INTERVAL,
    OS_DEFAULT
  }

  public static BrokerConfig load(Path configFile) throws IOException {
    Properties props = new Properties();
    if (configFile != null && Files.exists(configFile)) {
      try (InputStream in = Files.newInputStream(configFile)) {
        props.load(in);
      }
    }

    String host = envOrProp("BROKER_HOST", props, "broker.host", "0.0.0.0");
    int port = Integer.parseInt(envOrProp("BROKER_PORT", props, "broker.port", "9092"));
    Path dataDir = Path.of(envOrProp("DATA_DIR", props, "broker.data.dir", "./data"));
    FlushPolicy flushPolicy =
        FlushPolicy.valueOf(envOrProp("FLUSH_POLICY", props, "broker.flush.policy", "EVERY_WRITE"));
    long flushIntervalMs =
        Long.parseLong(envOrProp("FLUSH_INTERVAL_MS", props, "broker.flush.interval.ms", "1000"));
    long segmentMaxBytes =
        Long.parseLong(
            envOrProp("SEGMENT_MAX_BYTES", props, "broker.segment.max.bytes", "67108864"));
    long sessionTimeoutMs =
        Long.parseLong(
            envOrProp("SESSION_TIMEOUT_MS", props, "broker.session.timeout.ms", "10000"));

    return new BrokerConfig(
        host, port, dataDir, flushPolicy, flushIntervalMs, segmentMaxBytes, sessionTimeoutMs);
  }

  private static String envOrProp(
      String envKey, Properties props, String propKey, String defaultValue) {
    String env = System.getenv(envKey);
    if (env != null && !env.isBlank()) {
      return env.trim();
    }
    return props.getProperty(propKey, defaultValue).trim();
  }
}
