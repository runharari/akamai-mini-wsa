package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Asks the Kafka Streams state stores directly ("interactive queries"): no database involved.
 * Assumes one alerting instance. With several, each would hold only some count keys, and a request
 * would have to be forwarded to the instance that owns the key (KafkaStreams#queryMetadataForKey).
 */
@Component
@Profile({"alerting", "default"})
public class KafkaStreamsAlertStateReader implements AlertStateReader {

    private final StreamsBuilderFactoryBean streams;

    public KafkaStreamsAlertStateReader(StreamsBuilderFactoryBean streams) {
        this.streams = streams;
    }

    @Override
    public List<AlertRule> rules() {
        return whenReady(() -> {
            List<AlertRule> rules = new ArrayList<>();
            try (KeyValueIterator<String, AlertRule> all =
                         store(AlertingTopology.RULES_STORE, QueryableStoreTypes.<String, AlertRule>keyValueStore()).all()) {
                all.forEachRemaining(entry -> rules.add(entry.value));
            }
            return rules;
        });
    }

    @Override
    public long count(String countKey, Instant from, Instant to) {
        return whenReady(() -> {
            long total = 0;
            try (WindowStoreIterator<Long> minutes =
                         store(AlertingTopology.COUNTS_STORE, QueryableStoreTypes.<String, Long>windowStore()).fetch(countKey, from, to)) {
                while (minutes.hasNext()) {
                    total += minutes.next().value;
                }
            }
            return total;
        });
    }

    private <T> T store(String name, QueryableStoreType<T> type) {
        KafkaStreams kafkaStreams = streams.getKafkaStreams();
        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            throw new InvalidStateStoreException("Kafka Streams is not running yet");
        }
        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(name, type));
    }

    /** The stores can't be read during startup or a rebalance: that's a 503, not a bug. */
    private static <T> T whenReady(Supplier<T> read) {
        try {
            return read.get();
        } catch (InvalidStateStoreException e) {
            throw new ServiceUnavailableException("Alerting is starting or rebalancing, retry shortly", e);
        }
    }
}
