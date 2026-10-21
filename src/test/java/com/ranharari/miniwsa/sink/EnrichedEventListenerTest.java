package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.error.StorageException;
import com.ranharari.miniwsa.common.event.EnrichedEvent;
import com.ranharari.miniwsa.common.kafka.Topics;
import com.ranharari.miniwsa.common.metrics.PipelineMetrics;
import com.ranharari.miniwsa.support.TestEvents;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class EnrichedEventListenerTest {

    @Mock
    private EventWriter writer;
    @Mock
    private PipelineMetrics metrics;

    private final Clock clock = Clock.fixed(Instant.parse("2026-05-20T15:00:00Z"), ZoneOffset.UTC);

    private EnrichedEventListener listener() {
        return new EnrichedEventListener(writer, metrics, clock);
    }

    @Test
    void insertsWholeBatchOnceAndRecordsIt() {
        EnrichedEvent a = TestEvents.enriched("a");
        EnrichedEvent b = TestEvents.enriched("b");

        listener().onBatch(List.of(record(a), record(b)));

        verify(writer).insert(List.of(a, b));
        verify(metrics).sinkBatchInserted(2, Duration.ZERO);
        verifyNoMoreInteractions(writer, metrics);
    }

    @Test
    void skipsUnreadableMessagesAndInsertsTheRest() {
        EnrichedEvent a = TestEvents.enriched("a");

        listener().onBatch(List.of(record(null), record(a)));

        verify(metrics).sinkSkipped(1);
        verify(writer).insert(List.of(a));
        verify(metrics).sinkBatchInserted(1, Duration.ZERO);
    }

    @Test
    void doesNotInsertWhenEveryMessageIsUnreadable() {
        listener().onBatch(List.of(record(null)));

        verify(metrics).sinkSkipped(1);
        verifyNoInteractions(writer);
    }

    @Test
    void failedInsertIsCountedAndRethrownSoTheBatchIsRetried() {
        StorageException failure = new StorageException("ClickHouse down", new RuntimeException());
        doThrow(failure).when(writer).insert(any());

        assertThatThrownBy(() -> listener().onBatch(List.of(record(TestEvents.enriched("a"))))).isSameAs(failure);

        verify(metrics).sinkInsertFailed();
        verifyNoMoreInteractions(metrics);
    }

    private static ConsumerRecord<String, EnrichedEvent> record(EnrichedEvent value) {
        return new ConsumerRecord<>(Topics.ENRICHED, 0, 0, "key", value);
    }
}
