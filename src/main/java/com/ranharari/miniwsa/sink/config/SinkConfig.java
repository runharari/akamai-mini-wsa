package com.ranharari.miniwsa.sink.config;

import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.sink.ClickHouseEventWriter;
import com.ranharari.miniwsa.sink.EnrichedEventListener;
import com.ranharari.miniwsa.sink.EventSink;
import com.ranharari.miniwsa.sink.EventWriter;
import com.ranharari.miniwsa.sink.FailedEventPublisher;
import com.ranharari.miniwsa.sink.KafkaFailedEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Clock;

/** The sink role: Kafka listener → EventSink → ClickHouse, with bad events parked on events.failed. */
@Configuration
@Profile({"sink", "default"})
public class SinkConfig {

    @Bean
    EventWriter eventWriter(JdbcTemplate jdbcTemplate) {
        return new ClickHouseEventWriter(jdbcTemplate);
    }

    @Bean
    FailedEventPublisher failedEventPublisher(KafkaTemplate<String, SecurityEvent> kafkaTemplate) {
        return new KafkaFailedEventPublisher(kafkaTemplate);
    }

    @Bean
    EventSink eventSink(EventWriter writer, FailedEventPublisher failedEvents, MetricsFactory metrics, Clock clock) {
        return new EventSink(writer, failedEvents, metrics, clock);
    }

    @Bean
    EnrichedEventListener enrichedEventListener(EventSink sink) {
        return new EnrichedEventListener(sink);
    }

    /**
     * A failed insert (e.g. ClickHouse down) is retried with growing pauses, up to 30 s apart, and
     * never given up: dropping a batch would lose events. Meanwhile events wait safely in Kafka.
     */
    @Bean
    DefaultErrorHandler sinkErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(1_000, 2.0);
        backOff.setMaxInterval(30_000);
        return new DefaultErrorHandler(backOff);
    }
}
