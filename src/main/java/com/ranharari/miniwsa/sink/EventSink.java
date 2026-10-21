package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.exception.Exceptions;
import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Stores one batch of enriched events with one insert. Called by {@link EnrichedEventListener} for
 * every poll; the Kafka offsets are committed only after this returns, so a failed insert is retried,
 * never lost (at-least-once; duplicates are merged by the table's ReplacingMergeTree).
 * <p>
 * A failed insert means one of two things. ClickHouse is down: rethrow, and the batch is retried
 * until it's back. ClickHouse is up: some event in the batch is bad, so insert the events one by one
 * and park the ones that fail on the failed topic, instead of retrying the batch forever.
 */
public class EventSink {

    private static final Logger log = LoggerFactory.getLogger(EventSink.class);

    private final EventWriter writer;
    private final FailedEventPublisher failedEvents;
    private final Clock clock;

    private final DistributionSummary batchSize;
    private final Timer insertTime;
    private final Counter insertFailures;
    private final Counter skippedMessages;
    private final Counter parkedEvents;

    public EventSink(EventWriter writer, FailedEventPublisher failedEvents, MetricsFactory metrics, Clock clock) {
        this.writer = writer;
        this.failedEvents = failedEvents;
        this.clock = clock;
        this.batchSize = metrics.summary("sink.batch.size", "Events per ClickHouse insert");
        this.insertTime = metrics.timer("sink.insert", "Time of one ClickHouse batch insert");
        this.insertFailures = metrics.counter("sink.insert.failures", "Failed ClickHouse inserts (the batch is retried)");
        this.skippedMessages = metrics.counter("sink.skipped", "Unreadable messages skipped by the sink");
        this.parkedEvents = metrics.counter("sink.failed", "Events ClickHouse refused, sent to the failed topic");
    }

    /** A null entry is a message that couldn't be read (see ErrorHandlingDeserializer): it's skipped. */
    public void store(List<EnrichedEvent> messages) {
        List<EnrichedEvent> events = messages.stream()
                .filter(event -> event != null)
                .toList();
        int skipped = messages.size() - events.size();
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
            if (!writer.isAvailable()) {
                throw e; // ClickHouse is down: the container's error handler logs it and retries with backoff
            }
            insertOneByOne(events);
            return;
        }
        Duration took = Duration.between(start, clock.instant());
        batchSize.record(events.size());
        insertTime.record(took);
        log.info("Sink inserted events={} insertMs={}", events.size(), took.toMillis());
    }

    /** ClickHouse is up but refused the batch: store the good events, park the bad ones. */
    private void insertOneByOne(List<EnrichedEvent> events) {
        for (EnrichedEvent event : events) {
            try {
                writer.insert(List.of(event));
            } catch (StorageException e) {
                if (!writer.isAvailable()) {
                    throw e; // went down meanwhile: retry the whole batch (rows already stored are merged away)
                }
                failedEvents.publish(event.event(), e);
                parkedEvents.increment();
                log.warn("Sink parked event on the failed topic eventId={} reason={}", event.event().eventId(),
                        Exceptions.rootCauseMessage(e));
            }
        }
    }
}
