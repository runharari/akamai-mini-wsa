package com.ranharari.miniwsa.ingest.config;

import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.ingest.DefaultIngestService;
import com.ranharari.miniwsa.ingest.EventPublisher;
import com.ranharari.miniwsa.ingest.EventRequestParser;
import com.ranharari.miniwsa.ingest.IngestController;
import com.ranharari.miniwsa.ingest.IngestService;
import com.ranharari.miniwsa.ingest.JsonEventRequestParser;
import com.ranharari.miniwsa.ingest.KafkaEventPublisher;
import com.ranharari.miniwsa.ingest.RequestLimits;
import jakarta.validation.Validator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

/** The ingest role: request parsing and validation, publishing to Kafka, and the service between them. */
@Configuration
@Profile({"ingest", "default"})
@EnableConfigurationProperties(IngestProperties.class)
public class IngestConfig {

    @Bean
    EventRequestParser eventRequestParser(JsonMapper jsonMapper, Validator validator, IngestProperties properties,
                                          Clock clock) {
        RequestLimits limits = new RequestLimits(properties.maxBatchSize(), properties.maxEventAge(),
                properties.maxClockSkew());
        return new JsonEventRequestParser(jsonMapper, validator, limits, clock);
    }

    @Bean
    EventPublisher eventPublisher(KafkaTemplate<String, SecurityEvent> kafkaTemplate, IngestProperties properties) {
        return new KafkaEventPublisher(kafkaTemplate, properties.publishTimeout());
    }

    @Bean
    IngestService ingestService(EventRequestParser parser, EventPublisher publisher, MetricsFactory metrics, Clock clock) {
        return new DefaultIngestService(parser, publisher, metrics, clock);
    }

    @Bean
    IngestController ingestController(IngestService ingestService) {
        return new IngestController(ingestService);
    }
}
