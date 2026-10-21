package com.ranharari.miniwsa.processor;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.enrich.Enricher;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow.History;
import com.ranharari.miniwsa.processor.EnrichProcessor.EnrichMeters;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

/**
 * The Kafka Streams pipeline: events.raw → enrich (with the per-IP state store) → events.enriched.
 * Plain class (no Spring) so tests can run it in a TopologyTestDriver without a broker.
 */
public class EnrichmentTopology {

    /** How the two topics and the per-IP history in the state store are (de)serialized. */
    public record TopologySerdes(Serde<SecurityEvent> raw, Serde<EnrichedEvent> enriched, Serde<History> history) {
    }

    private final TopologySerdes serdes;
    private final RepeatOffenderWindow window;
    private final Enricher enricher;
    private final EnrichMeters meters;

    public EnrichmentTopology(TopologySerdes serdes, RepeatOffenderWindow window, Enricher enricher, MetricsFactory metrics) {
        this.serdes = serdes;
        this.window = window;
        this.enricher = enricher;
        this.meters = new EnrichMeters(
                metrics.counter("enrich.events", "Events enriched by the processor"),
                metrics.counter("enrich.repeat_offenders", "Events that got the repeat-offender bonus"),
                metrics.summary("enrich.threat_score", "Threat scores of enriched events"));
    }

    /** Adds the pipeline to the builder and returns the enriched stream. */
    public KStream<String, EnrichedEvent> build(StreamsBuilder builder) {
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(EnrichProcessor.STORE), Serdes.String(), serdes.history()));

        KStream<String, EnrichedEvent> enriched = builder
                .stream(Topics.RAW, Consumed.with(Serdes.String(), serdes.raw())
                        .withTimestampExtractor(new EventTimeExtractor()))
                .processValues(() -> new EnrichProcessor(window, enricher, meters), EnrichProcessor.STORE);
        enriched.to(Topics.ENRICHED, Produced.with(Serdes.String(), serdes.enriched()));
        return enriched;
    }
}
