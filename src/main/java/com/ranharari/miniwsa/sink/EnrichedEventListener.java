package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Reads enriched events in batches and writes each batch to ClickHouse with one insert.
 * Offsets are committed only after this method returns, so a failed insert is retried, never lost
 * (at-least-once; duplicates are merged by the table's ReplacingMergeTree).
 */
@Component
@Profile({"sink", "default"})
public class EnrichedEventListener {

    private static final Logger log = LoggerFactory.getLogger(EnrichedEventListener.class);

    private final EventWriter writer;
    private final Clock clock;

    private final DistributionSummary batchSize;
    private final Timer insertTime;
    private final Counter insertFailures;
    private final Counter skippedMessages;

    public EnrichedEventListener(EventWriter writer, MetricsFactory metrics, Clock clock) {
        this.writer = writer;
        this.clock = clock;
        this.batchSize = metrics.summary("sink.batch.size", "Events per ClickHouse insert");
        this.insertTime = metrics.timer("sink.insert", "Time of one ClickHouse batch insert");
        this.insertFailures = metrics.counter("sink.insert.failures", "Failed ClickHouse inserts (the batch is retried)");
        this.skippedMessages = metrics.counter("sink.skipped", "Unreadable messages skipped by the sink");
    }

    @KafkaListener(id = "clickhouse-sink", topics = Topics.ENRICHED, batch = "true",
            concurrency = "${wsa.sink.concurrency:3}")
    public void onBatch(List<ConsumerRecord<String, EnrichedEvent>> records) {
        // A null value means the message couldn't be read (see ErrorHandlingDeserializer): skip it.
        List<EnrichedEvent> events = records.stream()
                .map(ConsumerRecord::value)
                .filter(event -> event != null)
                .toList();
        int skipped = records.size() - events.size();
        if (skipped > 0) {
            skippedMessages.increment(skipped);
            log.warn("Sink skipped unreadable messages count={}", skipped);
        }
        if (events.isEmpty()) {
            return;
        }

        Instant start = clock.instant();
        try {
            writer.insert(events);
        } catch (StorageException e) {
            insertFailures.increment();
            throw e; // the container's error handler logs it and retries the batch with backoff
        }
        Duration took = Duration.between(start, clock.instant());
        batchSize.record(events.size());
        insertTime.record(took);
        log.info("Sink inserted events={} insertMs={}", events.size(), took.toMillis());
    }
}
