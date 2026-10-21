package com.ranharari.miniwsa.sink.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param concurrency  listener threads (at most the topic's partition count is useful); read by the
 *                     {@code @KafkaListener} through a placeholder
 * @param retryInitial pause before the first retry of a failed insert
 * @param retryMax     longest pause between retries
 */
@ConfigurationProperties("wsa.sink")
public record SinkProperties(
        @DefaultValue("3") int concurrency,
        @DefaultValue("1s") Duration retryInitial,
        @DefaultValue("30s") Duration retryMax) {
}
