package com.ranharari.miniwsa.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/** Registers meters in a Micrometer MeterRegistry (Spring's, which /actuator/prometheus exposes). */
public class MicrometerMetricsFactory implements MetricsFactory {

    private static final String PREFIX = "wsa.";

    private final MeterRegistry registry;

    public MicrometerMetricsFactory(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Counter counter(String name, String description, String... tags) {
        return Counter.builder(PREFIX + name).description(description).tags(tags).register(registry);
    }

    @Override
    public Timer timer(String name, String description, String... tags) {
        return Timer.builder(PREFIX + name).description(description).tags(tags).register(registry);
    }

    @Override
    public DistributionSummary summary(String name, String description, String... tags) {
        return DistributionSummary.builder(PREFIX + name).description(description).tags(tags).register(registry);
    }
}
