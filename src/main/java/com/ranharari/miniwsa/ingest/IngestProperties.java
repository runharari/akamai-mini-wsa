package com.ranharari.miniwsa.ingest;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param maxBatchSize   most events accepted in one request
 * @param publishTimeout how long to wait for Kafka acks before answering 503
 */
@ConfigurationProperties("wsa.ingest")
public record IngestProperties(
        @DefaultValue("5000") int maxBatchSize,
        @DefaultValue("10s") Duration publishTimeout) {
}
