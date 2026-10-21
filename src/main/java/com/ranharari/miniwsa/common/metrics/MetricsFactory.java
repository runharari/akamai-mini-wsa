package com.ranharari.miniwsa.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;

/**
 * Creates the meters a component needs. Each component creates its own meters in its constructor
 * and updates them where things happen. All names get the {@code wsa.} prefix.
 * Tests pass a factory backed by an in-memory registry and assert the meter values.
 */
public interface MetricsFactory {

    /** @param tags alternating key/value pairs, e.g. "result", "accepted" */
    Counter counter(String name, String description, String... tags);

    Timer timer(String name, String description, String... tags);

    DistributionSummary summary(String name, String description, String... tags);
}
