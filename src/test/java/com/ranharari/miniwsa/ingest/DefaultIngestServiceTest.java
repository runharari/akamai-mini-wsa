package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.exception.PublishFailedException;
import com.ranharari.miniwsa.common.exception.Violation;
import com.ranharari.miniwsa.common.metrics.MicrometerMetricsFactory;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.support.TestEvents;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultIngestServiceTest {

    private static final Instant NOW = Instant.parse("2026-05-20T15:00:00Z");

    @Mock
    private EventRequestParser parser;
    @Mock
    private EventPublisher publisher;

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
    private final JsonNode body = JsonMapper.builder().build().createArrayNode();

    private DefaultIngestService service() {
        return new DefaultIngestService(parser, publisher, new MicrometerMetricsFactory(meters), clock);
    }

    @Test
    @SuppressWarnings("unchecked")
    void stampsReceivedAtPublishesOnceAndCountsAcceptedEvents() {
        SecurityEvent a = TestEvents.event("a", "1.1.1.1", NOW.minusSeconds(60));
        SecurityEvent b = TestEvents.event("b", "2.2.2.2", NOW.minusSeconds(30));
        when(parser.parse(body)).thenReturn(List.of(a, b));

        int accepted = service().ingest(body);

        assertThat(accepted).isEqualTo(2);
        ArgumentCaptor<List<SecurityEvent>> published = ArgumentCaptor.forClass(List.class);
        verify(publisher).publish(published.capture());
        assertThat(published.getValue()).extracting(SecurityEvent::eventId).containsExactly("a", "b");
        assertThat(published.getValue()).extracting(SecurityEvent::receivedAt).containsOnly(NOW);

        assertThat(eventsCounted("accepted")).isEqualTo(2);
        assertThat(eventsCounted("rejected")).isZero();
        assertThat(meters.get("wsa.ingest.publish").timer().count()).isEqualTo(1);
        assertThat(meters.get("wsa.ingest.publish.failures").counter().count()).isZero();
    }

    @Test
    void invalidRequestPublishesNothingAndCountsRejectedEvents() {
        JsonNode twoEvents = JsonMapper.builder().build().createArrayNode().add(1).add(2);
        InvalidRequestException invalid = new InvalidRequestException(Violation.ofEvent(0, "action", "bad"));
        when(parser.parse(twoEvents)).thenThrow(invalid);

        assertThatThrownBy(() -> service().ingest(twoEvents)).isSameAs(invalid);

        verifyNoInteractions(publisher);
        assertThat(eventsCounted("rejected")).isEqualTo(2);
        assertThat(eventsCounted("accepted")).isZero();
    }

    @Test
    void publishFailureIsRethrownAndCountedAsFailureNotAccepted() {
        when(parser.parse(body)).thenReturn(List.of(TestEvents.event("a", "1.1.1.1", NOW)));
        PublishFailedException failure = new PublishFailedException("Kafka down", new RuntimeException());
        doThrow(failure).when(publisher).publish(any());

        assertThatThrownBy(() -> service().ingest(body)).isSameAs(failure);

        assertThat(meters.get("wsa.ingest.publish.failures").counter().count()).isEqualTo(1);
        assertThat(eventsCounted("accepted")).isZero();
    }

    private double eventsCounted(String result) {
        return meters.get("wsa.ingest.events").tag("result", result).counter().count();
    }
}
