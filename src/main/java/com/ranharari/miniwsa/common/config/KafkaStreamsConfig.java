package com.ranharari.miniwsa.common.config;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

/**
 * The Kafka Streams app of the roles that use it. Each role adds its own topology to it: processor
 * (enrichment) and alerting (event counts). With no profile both run in one app; in Docker each role
 * is its own app (own application id). Threads, application id and the error handlers are set in
 * application.yml under spring.kafka.streams.
 */
@Configuration
@EnableKafkaStreams
@Profile({"processor", "alerting", "default"})
public class KafkaStreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsConfig.class);

    /** A bug in processing shouldn't stop the app for good: log it and start a fresh thread. */
    @Bean
    StreamsBuilderFactoryBeanConfigurer replaceDeadThreads() {
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(e -> {
            log.error("Kafka Streams thread died, replacing it", e);
            return StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }
}
