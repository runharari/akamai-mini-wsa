package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.error.InvalidRequestException;
import com.ranharari.miniwsa.common.error.PublishFailedException;
import com.ranharari.miniwsa.common.event.SecurityEvent;
import com.ranharari.miniwsa.common.metrics.PipelineMetrics;
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
    private final PipelineMetrics metrics;
    private final Clock clock;

    public DefaultIngestService(EventRequestParser parser, EventPublisher publisher, PipelineMetrics metrics, Clock clock) {
        this.parser = parser;
        this.publisher = publisher;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public int ingest(JsonNode body) {
        List<SecurityEvent> events;
        try {
            events = parser.parse(body);
        } catch (InvalidRequestException e) {
            metrics.ingestRejected(eventCount(body));
            throw e;
        }

        Instant receivedAt = clock.instant();
        List<SecurityEvent> stamped = events.stream().map(event -> event.withReceivedAt(receivedAt)).toList();

        Instant start = clock.instant();
        try {
            publisher.publish(stamped);
        } catch (PublishFailedException e) {
            metrics.ingestPublishFailed();
            throw e;
        }
        Duration publishTime = Duration.between(start, clock.instant());

        metrics.ingestPublished(publishTime);
        metrics.ingestAccepted(stamped.size());
        log.info("Ingest accepted events={} publishMs={}", stamped.size(), publishTime.toMillis());
        return stamped.size();
    }

    private static int eventCount(JsonNode body) {
        return body != null && body.isArray() ? body.size() : 1;
    }
}
