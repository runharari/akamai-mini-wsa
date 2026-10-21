package com.ranharari.miniwsa.processor;

import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.common.config.Topics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * The Kafka Streams pipeline: events.raw → enrich → events.enriched.
 * Plain class (no Spring) so tests can build it into a TopologyTestDriver without a broker.
 */
public class EnrichmentTopology {

    private final Serde<SecurityEvent> rawSerde;
    private final Serde<EnrichedEvent> enrichedSerde;

    public EnrichmentTopology(Serde<SecurityEvent> rawSerde, Serde<EnrichedEvent> enrichedSerde) {
        this.rawSerde = rawSerde;
        this.enrichedSerde = enrichedSerde;
    }

    /** Adds the pipeline to the builder and returns the enriched stream. */
    public KStream<String, EnrichedEvent> build(StreamsBuilder builder) {
        KStream<String, EnrichedEvent> enriched = builder
                .stream(Topics.RAW, Consumed.with(Serdes.String(), rawSerde))
                // Scoring arrives in v0.2; for now events pass through unscored.
                .mapValues(event -> new EnrichedEvent(event, "", 0));
        enriched.to(Topics.ENRICHED, Produced.with(Serdes.String(), enrichedSerde));
        return enriched;
    }
}
