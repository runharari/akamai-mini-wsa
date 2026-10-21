package com.ranharari.miniwsa.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param maxBatchSize   most events accepted in one request
 * @param publishTimeout how long to wait for Kafka acks before answering 503
 * @param maxEventAge    oldest event timestamp accepted (raise it for a one-off backfill)
 * @param maxClockSkew   how far in the future an event timestamp may be (clients' clocks differ a bit)
 */
@ConfigurationProperties("wsa.ingest")
public record IngestProperties(
        @DefaultValue("5000") int maxBatchSize,
        @DefaultValue("10s") Duration publishTimeout,
        @DefaultValue("365d") Duration maxEventAge,
        @DefaultValue("5m") Duration maxClockSkew) {
}
