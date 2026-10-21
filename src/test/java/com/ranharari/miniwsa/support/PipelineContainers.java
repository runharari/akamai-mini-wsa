package com.ranharari.miniwsa.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Kafka + ClickHouse containers shared by every Spring Boot integration test (singletons:
 * started once per test JVM). Each test class calls {@link #register} from its own
 * {@code @DynamicPropertySource}.
 */
public final class PipelineContainers {

    public static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2");

    static {
        KAFKA.start();
    }

    private PipelineContainers() {
    }

    public static void register(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", ClickHouseTestSupport.CLICKHOUSE::getJdbcUrl);
        registry.add("spring.datasource.username", ClickHouseTestSupport.CLICKHOUSE::getUsername);
        registry.add("spring.datasource.password", ClickHouseTestSupport.CLICKHOUSE::getPassword);
        // Fresh Streams state per test context, so no repeat-offender memory leaks between runs.
        registry.add("spring.kafka.streams.application-id", () -> "it-" + UUID.randomUUID());
        registry.add("spring.kafka.streams.state-dir", PipelineContainers::tempDir);
    }

    private static String tempDir() {
        try {
            return Files.createTempDirectory("kafka-streams-").toString();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
