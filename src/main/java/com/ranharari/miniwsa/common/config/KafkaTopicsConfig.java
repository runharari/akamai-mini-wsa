package com.ranharari.miniwsa.common.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Creates the pipeline topics at startup (if they don't exist yet). */
@Configuration
public class KafkaTopicsConfig {

    /** Also the most processor / sink threads that can work in parallel. */
    private static final int PARTITIONS = 6;

    /** One local broker, so no replication. Production would use 3. */
    private static final int REPLICAS = 1;

    @Bean
    NewTopic rawTopic() {
        return TopicBuilder.name(Topics.RAW).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic enrichedTopic() {
        return TopicBuilder.name(Topics.ENRICHED).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic failedTopic() {
        return TopicBuilder.name(Topics.FAILED).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    /** Few, small messages, read whole by every alerting instance: one partition is plenty. */
    @Bean
    NewTopic alertRulesTopic() {
        return TopicBuilder.name(Topics.ALERT_RULES).partitions(1).replicas(REPLICAS).compact().build();
    }
}
