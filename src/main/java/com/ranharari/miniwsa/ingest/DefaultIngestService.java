package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.exception.PublishFailedException;
import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
@Profile({"ingest", "default"})
public class DefaultIngestService implements IngestService {

    private static final Logger log = LoggerFactory.getLogger(DefaultIngestService.class);

    private final EventRequestParser parser;
    private final EventPublisher publisher;
    private final Clock clock;

    private final Counter acceptedEvents;
    private final Counter rejectedEvents;
    private final Timer publishTime;
    private final Counter publishFailures;

    public DefaultIngestService(EventRequestParser parser, EventPublisher publisher, MetricsFactory metrics, Clock clock) {
        this.parser = parser;
        this.publisher = publisher;
        this.clock = clock;
        this.acceptedEvents = metrics.counter("ingest.events", "Events accepted by the ingest API", "result", "accepted");
        this.rejectedEvents = metrics.counter("ingest.events", "Events rejected by validation", "result", "rejected");
        this.publishTime = metrics.timer("ingest.publish", "Time to get Kafka acks for one ingest request");
        this.publishFailures = metrics.counter("ingest.publish.failures", "Ingest requests that failed to publish (503)");
    }

    @Override
    public int ingest(JsonNode body) {
        List<SecurityEvent> events;
        try {
            events = parser.parse(body);
        } catch (InvalidRequestException e) {
            rejectedEvents.increment(eventCount(body));
            throw e;
        }

        Instant receivedAt = clock.instant();
        List<SecurityEvent> stamped = events.stream().map(event -> event.withReceivedAt(receivedAt)).toList();

        Instant start = clock.instant();
        try {
            publisher.publish(stamped);
        } catch (PublishFailedException e) {
            publishFailures.increment();
            throw e;
        }
        Duration took = Duration.between(start, clock.instant());

        publishTime.record(took);
        acceptedEvents.increment(stamped.size());
        log.info("Ingest accepted events={} publishMs={}", stamped.size(), took.toMillis());
        return stamped.size();
    }

    private static int eventCount(JsonNode body) {
        return body != null && body.isArray() ? body.size() : 1;
    }
}
