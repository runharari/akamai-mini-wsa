package com.ranharari.miniwsa.processor;

import com.ranharari.miniwsa.common.event.EnrichedEvent;
import com.ranharari.miniwsa.common.event.SecurityEvent;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

/**
 * Runs the enrichment topology. Threads, application id and the deserialization handler
 * (log and skip bad messages) are set in application.yml under spring.kafka.streams.
 */
@Configuration
@EnableKafkaStreams
@Profile({"processor", "default"})
public class ProcessorConfig {

    private static final Logger log = LoggerFactory.getLogger(ProcessorConfig.class);

    @Bean
    EnrichmentTopology enrichmentTopology(JsonMapper jsonMapper) {
        return new EnrichmentTopology(
                new JacksonJsonSerde<>(SecurityEvent.class, jsonMapper).noTypeInfo().ignoreTypeHeaders(),
                new JacksonJsonSerde<>(EnrichedEvent.class, jsonMapper).noTypeInfo().ignoreTypeHeaders());
    }

    /** Spring passes the StreamsBuilder here before the streams start. */
    @Bean
    KStream<String, EnrichedEvent> enrichmentPipeline(StreamsBuilder builder, EnrichmentTopology topology) {
        return topology.build(builder);
    }

    /** A bug in processing shouldn't stop the processor for good: log it and start a fresh thread. */
    @Bean
    StreamsBuilderFactoryBeanConfigurer replaceDeadThreads() {
        return factoryBean -> factoryBean.setStreamsUncaughtExceptionHandler(e -> {
            log.error("Kafka Streams thread died, replacing it", e);
            return StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }
}
