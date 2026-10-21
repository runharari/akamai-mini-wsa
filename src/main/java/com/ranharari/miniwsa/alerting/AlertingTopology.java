package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.List;

/**
 * The alerting Kafka Streams pipeline. Two state stores, both queried by {@code GET /v1/alerts/evaluate}:
 * <ul>
 *   <li>{@value #RULES_STORE}: every rule from the compacted alert.rules topic (a GlobalKTable: each
 *       instance has all rules).</li>
 *   <li>{@value #COUNTS_STORE}: events per minute per {@link CountKey}, kept for a day. Counts are kept for
 *       every category and config, not only for existing rules, so a new rule sees the past day at once.</li>
 * </ul>
 * Plain class (no Spring) so tests can run it in a TopologyTestDriver without a broker.
 */
public class AlertingTopology {

    public static final String RULES_STORE = "alert-rules";
    public static final String COUNTS_STORE = "alert-counts";

    static final Duration BUCKET = Duration.ofMinutes(1);
    /**
     * How late an event may arrive and still be counted in its minute. Kafka Streams drops events
     * older than the newest one it has seen minus this. Backfills, replays and generator runs send
     * events hours late, and anything within the longest rule window can still make a rule fire.
     */
    private static final Duration GRACE = Duration.ofMinutes(CountKey.MAX_WINDOW_MINUTES);
    /** Keep each minute's count until it can neither change nor matter. */
    private static final Duration RETENTION = GRACE.plus(BUCKET);

    private final Serde<EnrichedEvent> enrichedSerde;
    private final Serde<AlertRule> ruleSerde;

    public AlertingTopology(Serde<EnrichedEvent> enrichedSerde, Serde<AlertRule> ruleSerde) {
        this.enrichedSerde = enrichedSerde;
        this.ruleSerde = ruleSerde;
    }

    /** Adds the pipeline to the builder and returns the enriched-events stream it reads. */
    public KStream<String, EnrichedEvent> build(StreamsBuilder builder) {
        builder.globalTable(Topics.ALERT_RULES, Consumed.with(Serdes.String(), ruleSerde),
                Materialized.<String, AlertRule, KeyValueStore<Bytes, byte[]>>as(RULES_STORE)
                        .withKeySerde(Serdes.String()).withValueSerde(ruleSerde));

        KStream<String, EnrichedEvent> events = builder.stream(Topics.ENRICHED,
                Consumed.with(Serdes.String(), enrichedSerde).withTimestampExtractor(new EnrichedEventTimeExtractor()));

        // Re-key from clientIp to the count key. Kafka Streams moves the events through an internal
        // repartition topic, so all events of one key are counted by one task. Only the event id travels.
        events.flatMap((ip, enriched) -> List.of(
                        KeyValue.pair(CountKey.of(enriched.event().configId(), enriched.event().rule().category()),
                                enriched.event().eventId()),
                        KeyValue.pair(CountKey.allConfigs(enriched.event().rule().category()), enriched.event().eventId())))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeAndGrace(BUCKET, GRACE))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(COUNTS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withRetention(RETENTION));
        return events;
    }
}
