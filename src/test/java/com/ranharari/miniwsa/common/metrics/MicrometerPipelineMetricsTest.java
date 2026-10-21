package com.ranharari.miniwsa.common.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerPipelineMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerPipelineMetrics metrics = new MicrometerPipelineMetrics(registry);

    @Test
    void countsAcceptedAndRejectedEventsSeparately() {
        metrics.ingestAccepted(3);
        metrics.ingestAccepted(2);
        metrics.ingestRejected(4);

        assertThat(registry.get("wsa.ingest.events").tag("result", "accepted").counter().count()).isEqualTo(5);
        assertThat(registry.get("wsa.ingest.events").tag("result", "rejected").counter().count()).isEqualTo(4);
    }

    @Test
    void recordsPublishTimeAndFailures() {
        metrics.ingestPublished(Duration.ofMillis(12));
        metrics.ingestPublishFailed();

        assertThat(registry.get("wsa.ingest.publish").timer().totalTime(TimeUnit.MILLISECONDS)).isEqualTo(12);
        assertThat(registry.get("wsa.ingest.publish.failures").counter().count()).isEqualTo(1);
    }

    @Test
    void recordsSinkBatchSizeTimeFailuresAndSkips() {
        metrics.sinkBatchInserted(500, Duration.ofMillis(40));
        metrics.sinkInsertFailed();
        metrics.sinkSkipped(2);

        assertThat(registry.get("wsa.sink.batch.size").summary().totalAmount()).isEqualTo(500);
        assertThat(registry.get("wsa.sink.insert").timer().count()).isEqualTo(1);
        assertThat(registry.get("wsa.sink.insert.failures").counter().count()).isEqualTo(1);
        assertThat(registry.get("wsa.sink.skipped").counter().count()).isEqualTo(2);
    }
}
