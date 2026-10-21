package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.exception.Exceptions;
import com.ranharari.miniwsa.common.exception.PublishFailedException;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sends the original event (not the enriched one) to {@code events.failed}, keyed by clientIp like
 * events.raw, so it can be replayed into events.raw once the cause is fixed. The reason is in a header.
 */
public class KafkaFailedEventPublisher implements FailedEventPublisher {

    static final String ERROR_HEADER = "error.message";
    static final String STAGE_HEADER = "error.stage";
    private static final long TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, SecurityEvent> kafkaTemplate;

    public KafkaFailedEventPublisher(KafkaTemplate<String, SecurityEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(SecurityEvent event, Exception cause) {
        ProducerRecord<String, SecurityEvent> record = new ProducerRecord<>(Topics.FAILED, event.clientIp(), event);
        record.headers().add(STAGE_HEADER, bytes("sink"));
        // The root cause holds ClickHouse's own message, e.g. which value it couldn't parse.
        record.headers().add(ERROR_HEADER, bytes(Exceptions.rootCauseMessage(cause)));
        try {
            kafkaTemplate.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException("Interrupted while parking event " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new PublishFailedException("Could not park event " + event.eventId() + " on " + Topics.FAILED, e);
        }
    }

    private static byte[] bytes(String text) {
        return String.valueOf(text).getBytes(StandardCharsets.UTF_8);
    }
}
