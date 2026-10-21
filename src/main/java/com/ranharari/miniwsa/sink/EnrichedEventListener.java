package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.error.StorageException;
import com.ranharari.miniwsa.common.event.EnrichedEvent;
import com.ranharari.miniwsa.common.kafka.Topics;
import com.ranharari.miniwsa.common.metrics.PipelineMetrics;
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
    private final PipelineMetrics metrics;
    private final Clock clock;

    public EnrichedEventListener(EventWriter writer, PipelineMetrics metrics, Clock clock) {
        this.writer = writer;
        this.metrics = metrics;
        this.clock = clock;
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
            metrics.sinkSkipped(skipped);
            log.warn("Sink skipped unreadable messages count={}", skipped);
        }
        if (events.isEmpty()) {
            return;
        }

        Instant start = clock.instant();
        try {
            writer.insert(events);
        } catch (StorageException e) {
            metrics.sinkInsertFailed();
            throw e; // the container's error handler logs it and retries the batch with backoff
        }
        Duration insertTime = Duration.between(start, clock.instant());
        metrics.sinkBatchInserted(events.size(), insertTime);
        log.info("Sink inserted events={} insertMs={}", events.size(), insertTime.toMillis());
    }
}
