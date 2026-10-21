package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.error.FieldError;
import com.ranharari.miniwsa.common.error.InvalidRequestException;
import com.ranharari.miniwsa.common.error.PublishFailedException;
import com.ranharari.miniwsa.common.event.SecurityEvent;
import com.ranharari.miniwsa.common.metrics.PipelineMetrics;
import com.ranharari.miniwsa.support.TestEvents;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultIngestServiceTest {

    private static final Instant NOW = Instant.parse("2026-05-20T15:00:00Z");

    @Mock
    private EventRequestParser parser;
    @Mock
    private EventPublisher publisher;
    @Mock
    private PipelineMetrics metrics;

    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
    private final JsonNode body = JsonMapper.builder().build().createArrayNode();

    private DefaultIngestService service() {
        return new DefaultIngestService(parser, publisher, metrics, clock);
    }

    @Test
    @SuppressWarnings("unchecked")
    void stampsReceivedAtPublishesOnceAndRecordsMetrics() {
        SecurityEvent a = TestEvents.event("a", "1.1.1.1", NOW.minusSeconds(60));
        SecurityEvent b = TestEvents.event("b", "2.2.2.2", NOW.minusSeconds(30));
        when(parser.parse(body)).thenReturn(List.of(a, b));

        int accepted = service().ingest(body);

        assertThat(accepted).isEqualTo(2);
        ArgumentCaptor<List<SecurityEvent>> published = ArgumentCaptor.forClass(List.class);
        verify(publisher).publish(published.capture());
        assertThat(published.getValue()).extracting(SecurityEvent::eventId).containsExactly("a", "b");
        assertThat(published.getValue()).extracting(SecurityEvent::receivedAt).containsOnly(NOW);
        verify(metrics).ingestPublished(Duration.ZERO);
        verify(metrics).ingestAccepted(2);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    void invalidRequestIsCountedAndNothingIsPublished() {
        JsonNode twoEvents = JsonMapper.builder().build().createArrayNode().add(1).add(2);
        InvalidRequestException invalid = new InvalidRequestException(FieldError.ofEvent(0, "action", "bad"));
        when(parser.parse(twoEvents)).thenThrow(invalid);

        assertThatThrownBy(() -> service().ingest(twoEvents)).isSameAs(invalid);

        verifyNoInteractions(publisher);
        verify(metrics).ingestRejected(2);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    void publishFailureIsCountedAndRethrown() {
        when(parser.parse(body)).thenReturn(List.of(TestEvents.event("a", "1.1.1.1", NOW)));
        PublishFailedException failure = new PublishFailedException("Kafka down", new RuntimeException());
        doThrow(failure).when(publisher).publish(any());

        assertThatThrownBy(() -> service().ingest(body)).isSameAs(failure);

        verify(metrics).ingestPublishFailed();
        verify(metrics, never()).ingestAccepted(any(Integer.class));
        verifyNoMoreInteractions(metrics);
    }
}
