package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.common.metrics.MicrometerMetricsFactory;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.support.TestEvents;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnrichedEventListenerTest {

    @Mock
    private EventWriter writer;
    @Mock
    private FailedEventPublisher failedEvents;

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final Clock clock = Clock.fixed(Instant.parse("2026-05-20T15:00:00Z"), ZoneOffset.UTC);

    private EnrichedEventListener listener() {
        return new EnrichedEventListener(writer, failedEvents, new MicrometerMetricsFactory(meters), clock);
    }

    @Test
    void insertsWholeBatchOnceAndRecordsBatchSize() {
        EnrichedEvent a = TestEvents.enriched("a");
        EnrichedEvent b = TestEvents.enriched("b");

        listener().onBatch(List.of(record(a), record(b)));

        verify(writer).insert(List.of(a, b));
        verifyNoMoreInteractions(writer);
        assertThat(meters.get("wsa.sink.batch.size").summary().totalAmount()).isEqualTo(2);
        assertThat(meters.get("wsa.sink.insert").timer().count()).isEqualTo(1);
        assertThat(meters.get("wsa.sink.skipped").counter().count()).isZero();
    }

    @Test
    void skipsUnreadableMessagesCountsThemAndInsertsTheRest() {
        EnrichedEvent a = TestEvents.enriched("a");

        listener().onBatch(List.of(record(null), record(a)));

        verify(writer).insert(List.of(a));
        assertThat(meters.get("wsa.sink.skipped").counter().count()).isEqualTo(1);
        assertThat(meters.get("wsa.sink.batch.size").summary().totalAmount()).isEqualTo(1);
    }

    @Test
    void doesNotInsertWhenEveryMessageIsUnreadable() {
        listener().onBatch(List.of(record(null)));

        verifyNoInteractions(writer);
        assertThat(meters.get("wsa.sink.skipped").counter().count()).isEqualTo(1);
        assertThat(meters.get("wsa.sink.insert").timer().count()).isZero();
    }

    @Test
    void whenClickHouseIsDownTheFailureIsRethrownSoTheBatchIsRetried() {
        StorageException failure = new StorageException("ClickHouse down", new RuntimeException());
        doThrow(failure).when(writer).insert(any());
        when(writer.isAvailable()).thenReturn(false);

        assertThatThrownBy(() -> listener().onBatch(List.of(record(TestEvents.enriched("a"))))).isSameAs(failure);

        verifyNoInteractions(failedEvents);
        assertThat(meters.get("wsa.sink.insert.failures").counter().count()).isEqualTo(1);
        assertThat(meters.get("wsa.sink.insert").timer().count()).isZero();
    }

    @Test
    void whenClickHouseIsUpTheBadEventIsParkedAndTheOthersAreStored() {
        EnrichedEvent good = TestEvents.enriched("good");
        EnrichedEvent bad = TestEvents.enriched("bad");
        StorageException refused = new StorageException("refused", new RuntimeException("cannot parse"));
        // ClickHouse refuses any insert that contains the bad event.
        doAnswer(call -> {
            List<EnrichedEvent> rows = call.getArgument(0);
            if (rows.contains(bad)) {
                throw refused;
            }
            return null;
        }).when(writer).insert(any());
        when(writer.isAvailable()).thenReturn(true);

        listener().onBatch(List.of(record(good), record(bad)));

        verify(writer).insert(List.of(good));
        verify(failedEvents).publish(bad.event(), refused);
        verifyNoMoreInteractions(failedEvents);
        assertThat(meters.get("wsa.sink.failed").counter().count()).isEqualTo(1);
        assertThat(meters.get("wsa.sink.insert.failures").counter().count()).isEqualTo(1);
    }

    @Test
    void whenClickHouseGoesDownWhileInsertingOneByOneTheBatchIsRetried() {
        EnrichedEvent a = TestEvents.enriched("a");
        EnrichedEvent b = TestEvents.enriched("b");
        StorageException failure = new StorageException("failed", new RuntimeException());
        doThrow(failure).when(writer).insert(any());
        when(writer.isAvailable()).thenReturn(true, false);

        assertThatThrownBy(() -> listener().onBatch(List.of(record(a), record(b)))).isSameAs(failure);

        verifyNoInteractions(failedEvents);
    }

    private static ConsumerRecord<String, EnrichedEvent> record(EnrichedEvent value) {
        return new ConsumerRecord<>(Topics.ENRICHED, 0, 0, "key", value);
    }
}
