package com.ranharari.miniwsa.common.metrics;

import java.time.Duration;

/**
 * Our own pipeline metrics. Behind an interface so unit tests can verify that each code path
 * records what it should. Published as {@code wsa.*} meters (see {@link MicrometerPipelineMetrics}).
 */
public interface PipelineMetrics {

    void ingestAccepted(int events);

    void ingestRejected(int events);

    void ingestPublished(Duration duration);

    void ingestPublishFailed();

    void sinkBatchInserted(int events, Duration duration);

    void sinkInsertFailed();

    void sinkSkipped(int events);
}
