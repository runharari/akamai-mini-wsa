package com.ranharari.miniwsa.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class MicrometerPipelineMetrics implements PipelineMetrics {

    private final Counter ingestAccepted;
    private final Counter ingestRejected;
    private final Timer ingestPublish;
    private final Counter ingestPublishFailures;
    private final DistributionSummary sinkBatchSize;
    private final Timer sinkInsert;
    private final Counter sinkInsertFailures;
    private final Counter sinkSkipped;

    public MicrometerPipelineMetrics(MeterRegistry registry) {
        ingestAccepted = Counter.builder("wsa.ingest.events").tag("result", "accepted")
                .description("Events accepted by the ingest API").register(registry);
        ingestRejected = Counter.builder("wsa.ingest.events").tag("result", "rejected")
                .description("Events rejected by validation").register(registry);
        ingestPublish = Timer.builder("wsa.ingest.publish")
                .description("Time to get Kafka acks for one ingest request").register(registry);
        ingestPublishFailures = Counter.builder("wsa.ingest.publish.failures")
                .description("Ingest requests that failed to publish (503)").register(registry);
        sinkBatchSize = DistributionSummary.builder("wsa.sink.batch.size")
                .description("Events per ClickHouse insert").register(registry);
        sinkInsert = Timer.builder("wsa.sink.insert")
                .description("Time of one ClickHouse batch insert").register(registry);
        sinkInsertFailures = Counter.builder("wsa.sink.insert.failures")
                .description("Failed ClickHouse inserts (the batch is retried)").register(registry);
        sinkSkipped = Counter.builder("wsa.sink.skipped")
                .description("Unreadable messages skipped by the sink").register(registry);
    }

    @Override
    public void ingestAccepted(int events) {
        ingestAccepted.increment(events);
    }

    @Override
    public void ingestRejected(int events) {
        ingestRejected.increment(events);
    }

    @Override
    public void ingestPublished(Duration duration) {
        ingestPublish.record(duration);
    }

    @Override
    public void ingestPublishFailed() {
        ingestPublishFailures.increment();
    }

    @Override
    public void sinkBatchInserted(int events, Duration duration) {
        sinkBatchSize.record(events);
        sinkInsert.record(duration);
    }

    @Override
    public void sinkInsertFailed() {
        sinkInsertFailures.increment();
    }

    @Override
    public void sinkSkipped(int events) {
        sinkSkipped.increment(events);
    }
}
