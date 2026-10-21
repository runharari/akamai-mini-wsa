package com.ranharari.miniwsa.processor.config;

import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.enrich.Enricher;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow.History;
import com.ranharari.miniwsa.processor.EnrichmentTopology;
import com.ranharari.miniwsa.processor.EnrichmentTopology.TopologySerdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

/** Adds the enrichment topology to the Kafka Streams app (see KafkaStreamsConfig). */
@Configuration
@Profile({"processor", "default"})
public class ProcessorConfig {

    @Bean
    EnrichmentTopology enrichmentTopology(JsonMapper jsonMapper, RepeatOffenderWindow window, Enricher enricher,
                                          MetricsFactory metrics) {
        TopologySerdes serdes = new TopologySerdes(
                new JacksonJsonSerde<>(SecurityEvent.class, jsonMapper).noTypeInfo().ignoreTypeHeaders(),
                new JacksonJsonSerde<>(EnrichedEvent.class, jsonMapper).noTypeInfo().ignoreTypeHeaders(),
                new JacksonJsonSerde<>(History.class, jsonMapper).noTypeInfo().ignoreTypeHeaders());
        return new EnrichmentTopology(serdes, window, enricher, metrics);
    }

    /** Spring passes the StreamsBuilder here before the streams start. */
    @Bean
    KStream<String, EnrichedEvent> enrichmentPipeline(StreamsBuilder builder, EnrichmentTopology topology) {
        return topology.build(builder);
    }
}
