package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.exception.PublishFailedException;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.support.TestEvents;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaEventPublisherTest {

    private static final Instant T = Instant.parse("2026-05-20T14:32:10Z");

    @Mock
    private KafkaTemplate<String, SecurityEvent> kafkaTemplate;

    private KafkaEventPublisher publisher() {
        return new KafkaEventPublisher(kafkaTemplate, Duration.ofMillis(200));
    }

    @Test
    void sendsEachEventToRawTopicKeyedByClientIp() {
        SecurityEvent a = TestEvents.event("a", "1.1.1.1", T);
        SecurityEvent b = TestEvents.event("b", "2.2.2.2", T);
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(acked());

        publisher().publish(List.of(a, b));

        verify(kafkaTemplate).send(Topics.RAW, "1.1.1.1", a);
        verify(kafkaTemplate).send(Topics.RAW, "2.2.2.2", b);
    }

    @Test
    void failsWhenKafkaRejectsAnEvent() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> publisher().publish(List.of(TestEvents.event("a", "1.1.1.1", T))))
                .isInstanceOf(PublishFailedException.class)
                .hasMessageContaining("broker down");
    }

    @Test
    void failsWhenAcksDoNotArriveInTime() {
        when(kafkaTemplate.send(eq(Topics.RAW), anyString(), any())).thenReturn(new CompletableFuture<>());

        assertThatThrownBy(() -> publisher().publish(List.of(TestEvents.event("a", "1.1.1.1", T))))
                .isInstanceOf(PublishFailedException.class)
                .hasMessageContaining("did not ack");
    }

    @Test
    void failsWhenSendThrowsImmediately() {
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenThrow(new IllegalStateException("no metadata"));

        assertThatThrownBy(() -> publisher().publish(List.of(TestEvents.event("a", "1.1.1.1", T))))
                .isInstanceOf(PublishFailedException.class);
    }

    private static CompletableFuture<SendResult<String, SecurityEvent>> acked() {
        return CompletableFuture.completedFuture(null);
    }
}
