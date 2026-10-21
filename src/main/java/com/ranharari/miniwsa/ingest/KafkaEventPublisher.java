package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.error.PublishFailedException;
import com.ranharari.miniwsa.common.event.SecurityEvent;
import com.ranharari.miniwsa.common.kafka.Topics;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sends events to {@code events.raw}, keyed by clientIp: all events of one IP land on the same
 * partition, so one processor task sees all of them in order (needed by the repeat-offender window).
 * The producer uses acks=all, so a successful publish means Kafka stored the event.
 */
@Component
@Profile({"ingest", "default"})
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, SecurityEvent> kafkaTemplate;
    private final IngestProperties properties;

    public KafkaEventPublisher(KafkaTemplate<String, SecurityEvent> kafkaTemplate, IngestProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Override
    public void publish(List<SecurityEvent> events) {
        try {
            // Send all first (the producer batches them), then wait once for all the acks.
            CompletableFuture<?>[] acks = new CompletableFuture<?>[events.size()];
            for (int i = 0; i < events.size(); i++) {
                SecurityEvent event = events.get(i);
                CompletableFuture<SendResult<String, SecurityEvent>> ack =
                        kafkaTemplate.send(Topics.RAW, event.clientIp(), event);
                acks[i] = ack;
            }
            CompletableFuture.allOf(acks).get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException("Interrupted while publishing " + events.size() + " events", e);
        } catch (TimeoutException e) {
            throw new PublishFailedException("Kafka did not ack " + events.size() + " events within "
                    + properties.publishTimeout(), e);
        } catch (ExecutionException | RuntimeException e) {
            throw new PublishFailedException("Kafka rejected the events: " + e.getMessage(), e);
        }
    }
}
