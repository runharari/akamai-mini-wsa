package com.ranharari.miniwsa.sink;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
@Profile({"sink", "default"})
public class SinkConfig {

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
