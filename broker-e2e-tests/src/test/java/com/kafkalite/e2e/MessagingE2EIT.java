package com.kafkalite.e2e;

import com.kafkalite.client.admin.AdminClient;
import com.kafkalite.client.consumer.KafkaLiteConsumer;
import com.kafkalite.client.consumer.KafkaLiteConsumer.FetchResult;
import com.kafkalite.client.producer.KafkaLiteProducer;
import com.kafkalite.client.producer.KafkaLiteProducer.ProduceAck;
import com.kafkalite.client.producer.KafkaLiteProducer.ProducerRecord;
import com.kafkalite.core.config.BrokerConfig;
import com.kafkalite.core.config.BrokerConfig.FlushPolicy;
import com.kafkalite.core.network.BrokerServer;
import com.kafkalite.core.topic.TopicManager;
import com.kafkalite.protocol.error.ErrorCode;
import com.kafkalite.protocol.exception.BrokerException;
import com.kafkalite.protocol.model.Record;
import com.kafkalite.protocol.model.Response.TopicInfo;
import com.kafkalite.protocol.model.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessagingE2EIT {
    @TempDir Path tempDir;

    private BrokerConfig config(Path dir) {
        return new BrokerConfig(
                "127.0.0.1", 0, dir, FlushPolicy.EVERY_WRITE, 1000L, 64 * 1024L, 10_000L);
    }

    @Test
    @Tag("AC-01")
    @Tag("AC-02")
    @Tag("AC-03")
    @Tag("AC-04")
    void fullTcpPublishFetchAndRestartDurability() throws Exception {
        BrokerConfig cfg = config(tempDir);

        try (TopicManager tm = TopicManager.open(cfg, Clock.systemUTC());
             BrokerServer server = new BrokerServer(cfg, tm)) {
            server.start();
            int port = server.boundPort();

            try (AdminClient admin = new AdminClient("127.0.0.1", port);
                 KafkaLiteProducer producer = new KafkaLiteProducer("127.0.0.1", port);
                 KafkaLiteConsumer consumer = new KafkaLiteConsumer("127.0.0.1", port)) {

                TopicInfo created = admin.createTopic("orders", 2);
                assertThat(created.partitionCount()).isEqualTo(2);

                // Batch publish to explicit partition 0
                List<ProduceAck> acks =
                        producer.sendBatch(
                                "orders",
                                0,
                                List.of(
                                        new ProducerRecord(
                                                "k1".getBytes(StandardCharsets.UTF_8),
                                                "m0".getBytes(StandardCharsets.UTF_8)),
                                        new ProducerRecord(
                                                "k1".getBytes(StandardCharsets.UTF_8),
                                                "m1".getBytes(StandardCharsets.UTF_8))));
                assertThat(acks).extracting(ProduceAck::offset).containsExactly(0L, 1L);

                TopicPartition tp0 = new TopicPartition("orders", 0);
                FetchResult polled = consumer.poll(tp0, 10, 64 * 1024);
                assertThat(polled.highWatermark()).isEqualTo(2L);
                assertThat(polled.records()).extracting(Record::offset).containsExactly(0L, 1L);
                assertThat(consumer.position(tp0)).isEqualTo(2L);

                // Verify structured error over wire
                assertThatThrownBy(() -> admin.describeTopic("does-not-exist"))
                        .isInstanceOf(BrokerException.class)
                        .extracting(e -> ((BrokerException) e).errorCode())
                        .isEqualTo(ErrorCode.UNKNOWN_TOPIC);
            }
        }

        // Restart broker and verify durability over TCP
        try (TopicManager recoveredTm = TopicManager.open(cfg, Clock.systemUTC());
             BrokerServer recoveredServer = new BrokerServer(cfg, recoveredTm)) {
            recoveredServer.start();
            try (KafkaLiteConsumer consumer =
                         new KafkaLiteConsumer("127.0.0.1", recoveredServer.boundPort())) {
                FetchResult afterRestart = consumer.fetch("orders", 0, 0L, 10, 64 * 1024);
                assertThat(afterRestart.records())
                        .extracting(r -> new String(r.payload(), StandardCharsets.UTF_8))
                        .containsExactly("m0", "m1");
            }
        }
    }

    @Test
    @Tag("AC-10")
    void concurrentProducersOverTcpAssignUniqueOffsets() throws Exception {
        BrokerConfig cfg = config(tempDir);
        try (TopicManager tm = TopicManager.open(cfg, Clock.systemUTC());
             BrokerServer server = new BrokerServer(cfg, tm);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            server.start();
            int port = server.boundPort();

            try (AdminClient admin = new AdminClient("127.0.0.1", port)) {
                admin.createTopic("concurrent-tcp", 1);
            }

            List<Future<List<Long>>> futures = new ArrayList<>();
            for (int p = 0; p < 4; p++) {
                futures.add(
                        executor.submit(
                                () -> {
                                    List<Long> offsets = new ArrayList<>();
                                    try (KafkaLiteProducer producer =
                                                 new KafkaLiteProducer("127.0.0.1", port)) {
                                        for (int i = 0; i < 50; i++) {
                                            offsets.add(
                                                    producer.send(
                                                                    "concurrent-tcp",
                                                                    0,
                                                                    null,
                                                                    ("msg-" + i)
                                                                            .getBytes(
                                                                                    StandardCharsets
                                                                                            .UTF_8))
                                                            .offset());
                                        }
                                    }
                                    return offsets;
                                }));
            }

            List<Long> allOffsets = new ArrayList<>();
            for (Future<List<Long>> f : futures) {
                allOffsets.addAll(f.get());
            }
            assertThat(allOffsets).hasSize(200).doesNotHaveDuplicates();
        }
    }
}