package com.ranharari.miniwsa.alerting.config;

import com.ranharari.miniwsa.alerting.AlertRule;
import com.ranharari.miniwsa.alerting.AlertingTopology;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

/** Adds the alerting topology to the Kafka Streams app (see KafkaStreamsConfig). */
@Configuration
@Profile({"alerting", "default"})
public class AlertingConfig {

    @Bean
    AlertingTopology alertingTopology(JsonMapper jsonMapper) {
        return new AlertingTopology(
                new JacksonJsonSerde<>(EnrichedEvent.class, jsonMapper).noTypeInfo().ignoreTypeHeaders(),
                new JacksonJsonSerde<>(AlertRule.class, jsonMapper).noTypeInfo().ignoreTypeHeaders());
    }

    /** Spring passes the StreamsBuilder here before the streams start. */
    @Bean
    KStream<String, EnrichedEvent> alertingPipeline(StreamsBuilder builder, AlertingTopology topology) {
        return topology.build(builder);
    }
}
