# KafkaLite Broker

A durable, single-node message broker written in **Java** (targeting Java 21+) and containerized with **Docker**, inspired by Apache Kafka.

Built to explore append-only log storage, sparse offset indexing, `fsync` vs. OS page-cache durability trade-offs, binary TCP wire framing, consumer group rebalancing, and crash recovery.

---

## Architecture

```text
  Producer / Consumer / Admin Client (broker-client)
                        │
        Length-Prefixed Binary TCP Protocol (broker-protocol)
                        │
                 BrokerServer (Virtual Threads)
          ┌─────────────┼──────────────┬──────────────┐
          ▼             ▼              ▼              ▼
     TopicManager  GroupCoordinator  OffsetManager  BrokerMetrics
          │
     PartitionLog  (ReentrantReadWriteLock + FlushPolicy)
          │
     LogSegment    (.log append-only + CRC32C | .index sparse offsets)
```

### Multi-Module Layout

| Module | Responsibility |
|---|---|
| `broker-protocol` | Length-prefixed binary TCP framing (`WireCodec`), `CRC32C` record integrity (`RecordCodec`), request/response records, and protocol error codes. |
| `broker-core` | Storage engine (`PartitionLog`, `LogSegment`, `LogInspector`), topic/partition management (`TopicManager`), consumer group coordination (`GroupCoordinator`, `OffsetManager`), observability (`BrokerMetrics`), and TCP listener (`BrokerServer`). |
| `broker-client` | Producer (`KafkaLiteProducer`), stateful pull consumer with background virtual-thread heartbeats (`KafkaLiteConsumer`), admin utilities (`AdminClient`), and verification CLI (`ClientCli`). |
| `broker-e2e` | Integration tests (`CrashRecoveryE2EIT`) covering ungraceful `SIGKILL` restarts, torn-tail log truncation, and offset resumption, plus `BenchmarkRunner`. |

---

## Getting Started

### Build & Test

```bash
# Compile all modules and run unit + E2E integration tests
mvn verify
```

### Run the Broker Locally

```bash
java -cp broker-core/target/classes:broker-protocol/target/classes com.kafkalite.core.BrokerApplication
```

### Configuration

All settings can be supplied via `config/broker.properties` (overridable with `BROKER_CONFIG`) or environment variables:

| Environment Variable | Property Key | Default | Description |
|---|---|---|---|
| `BROKER_HOST` | `broker.host` | `0.0.0.0` | Network interface to bind. |
| `BROKER_PORT` | `broker.port` | `9092` | TCP listening port (`0` for ephemeral OS port). |
| `DATA_DIR` | `broker.data.dir` | `./data` | Root directory for segment logs, indexes, and metadata snapshots. |
| `FLUSH_POLICY` | `broker.flush.policy` | `EVERY_WRITE` | Durability mode: `EVERY_WRITE`, `INTERVAL`, or `OS_DEFAULT`. |
| `FLUSH_INTERVAL_MS` | `broker.flush.interval.ms` | `1000` | `fsync` interval in milliseconds when `FLUSH_POLICY=INTERVAL`. |
| `SEGMENT_MAX_BYTES` | `broker.segment.max.bytes` | `67108864` | Roll threshold per `.log` segment (`64 MiB`). |
| `SESSION_TIMEOUT_MS` | `broker.session.timeout.ms` | `10000` | Consumer group heartbeat timeout before triggering rebalance. |

### CLI & Benchmarking

```bash
# Inspect broker health, recovery stats, and consumer group lag
java -cp <classpath> com.kafkalite.client.admin.ClientCli 127.0.0.1 9092 status

# Inspect raw .log segment framing and CRC32C checksums on disk
java -cp <classpath> com.kafkalite.core.lifecycle.LogInspector ./data/my-topic/0/00000000000000000000.log

# Run the embedded benchmark suite (10,000 messages, 512 B payload)
java -cp <classpath> com.kafkalite.bench.BenchmarkRunner 10000 512
```

---

## Benchmark Results

Measured locally (`10,000` messages, `512 B` payload unless noted; `EVERY_WRITE` capped at `1,500` sync writes):

| Experiment | Messages | Throughput (msg/s) | MB/s | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| Producer (`EVERY_WRITE`, `batch=1`) | 1,500 | 233 | 0.11 | 4.005 | 6.087 | 7.870 |
| Producer (`INTERVAL`, `batch=1`) | 10,000 | 33,544 | 16.38 | 0.027 | 0.053 | 0.072 |
| Producer (`OS_DEFAULT`, `batch=1`) | 10,000 | 39,873 | 19.47 | 0.025 | 0.030 | 0.033 |
| Producer (`OS_DEFAULT`, `batch=16`) | 10,000 | 234,638 | 114.57 | 0.063 | 0.081 | 0.122 |
| Producer (`OS_DEFAULT`, `batch=64`) | 9,984 | 375,631 | 183.41 | 0.156 | 0.201 | 0.341 |
| Producer (`OS_DEFAULT`, `batch=256`) | 9,984 | 452,110 | 220.76 | 0.528 | 0.609 | 1.175 |
| Parallel Producers (4 partitions) | 10,000 | 64,939 | 31.71 | 0.055 | 0.105 | 0.126 |
| Consumer Fetch (4 partitions) | 10,000 | 438,235 | 213.98 | 0.936 | 1.221 | 1.838 |
| End-to-End Latency (produce+fetch) | 500 | 12,806 | 6.25 | 0.071 | 0.120 | 0.148 |

### Key Takeaways

- **`fsync` vs. Page Cache:** Per-write `FileChannel.force(true)` (`~4.0 ms` p50) limits unbatched writes to `233 msg/s`, while OS page-cache writes (`OS_DEFAULT`) reach `39,873 msg/s` (`~170x` higher).
- **Batching Impact:** Increasing batch size from `1` to `256` increases producer throughput by `11.3x` (`39.9k` $\rightarrow$ `452.1k msg/s` / `220.8 MB/s`) by amortizing TCP framing overhead.
- **Parallelism:** Per-partition locking scales unbatched 4-partition writes by `+63%`, while positional `FileChannel` reads sustain `438k msg/s` (`214 MB/s`).