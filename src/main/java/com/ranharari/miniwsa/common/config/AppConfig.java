package com.ranharari.miniwsa.common.config;

import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.metrics.MicrometerMetricsFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.cfg.EnumFeature;

import java.time.Clock;

/** Beans every role needs. */
@Configuration
public class AppConfig {

    /** Injected wherever "now" matters, so tests can use a fixed clock. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    MetricsFactory metricsFactory(MeterRegistry registry) {
        return new MicrometerMetricsFactory(registry);
    }

    /** Enums must be sent by name ("DENY"), never by position (0). */
    @Bean
    JsonMapperBuilderCustomizer strictEnums() {
        return builder -> builder.enable(EnumFeature.FAIL_ON_NUMBERS_FOR_ENUMS);
    }
}
