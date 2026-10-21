package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.exception.PublishFailedException;
import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.support.TestEvents;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaFailedEventPublisherTest {

    private static final SecurityEvent EVENT = TestEvents.event("a", "1.1.1.1", Instant.parse("2026-05-20T14:32:10Z"));
    private static final StorageException FAILURE =
            new StorageException("Insert failed", new RuntimeException("Cannot parse input"));

    @Mock
    private KafkaTemplate<String, SecurityEvent> kafkaTemplate;

    @Test
    @SuppressWarnings("unchecked")
    void sendsTheOriginalEventKeyedByIpWithTheRootCauseInAHeader() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        new KafkaFailedEventPublisher(kafkaTemplate).publish(EVENT, FAILURE);

        ArgumentCaptor<ProducerRecord<String, SecurityEvent>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(sent.capture());
        ProducerRecord<String, SecurityEvent> record = sent.getValue();
        assertThat(record.topic()).isEqualTo(Topics.FAILED);
        assertThat(record.key()).isEqualTo("1.1.1.1");
        assertThat(record.value()).isEqualTo(EVENT);
        assertThat(header(record, KafkaFailedEventPublisher.ERROR_HEADER)).isEqualTo("Cannot parse input");
        assertThat(header(record, KafkaFailedEventPublisher.STAGE_HEADER)).isEqualTo("sink");
    }

    @Test
    @SuppressWarnings("unchecked")
    void failsWhenKafkaDoesNotStoreIt() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> new KafkaFailedEventPublisher(kafkaTemplate).publish(EVENT, FAILURE))
                .isInstanceOf(PublishFailedException.class);
    }

    private static String header(ProducerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
