package com.ranharari.miniwsa.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param partitions partitions per topic; also the max useful number of processor/sink threads
 */
@ConfigurationProperties("wsa.kafka")
public record KafkaTopicProperties(@DefaultValue("6") int partitions) {
}
