package com.ranharari.miniwsa.alerting.config;

import com.ranharari.miniwsa.alerting.AlertRule;
import com.ranharari.miniwsa.alerting.AlertService;
import com.ranharari.miniwsa.alerting.AlertStateReader;
import com.ranharari.miniwsa.alerting.DefaultAlertService;
import com.ranharari.miniwsa.alerting.KafkaRulePublisher;
import com.ranharari.miniwsa.alerting.KafkaStreamsAlertStateReader;
import com.ranharari.miniwsa.alerting.RulePublisher;
import com.ranharari.miniwsa.alerting.AlertingTopology;
import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

/** The alerting role: its Kafka Streams topology, the rules publisher, the state reader and the service. */
@Configuration
@Profile({"alerting", "default"})
public class AlertingConfig {

    @Bean
    AlertingTopology alertingTopology(JsonMapper jsonMapper) {
        return new AlertingTopology(
                new JacksonJsonSerde<>(EnrichedEvent.class, jsonMapper).noTypeInfo().ignoreTypeHeaders(),
                new JacksonJsonSerde<>(AlertRule.class, jsonMapper).noTypeInfo().ignoreTypeHeaders());
    }

    @Bean
    RulePublisher rulePublisher(KafkaTemplate<String, AlertRule> kafkaTemplate) {
        return new KafkaRulePublisher(kafkaTemplate);
    }

    /** The reader asks for the running KafkaStreams at query time: it exists only once the app has started. */
    @Bean
    AlertStateReader alertStateReader(StreamsBuilderFactoryBean streams) {
        return new KafkaStreamsAlertStateReader(streams::getKafkaStreams);
    }

    @Bean
    AlertService alertService(RulePublisher publisher, AlertStateReader state, MetricsFactory metrics, Clock clock) {
        return new DefaultAlertService(publisher, state, metrics, clock);
    }

    /** Spring passes the StreamsBuilder here before the streams start. */
    @Bean
    KStream<String, EnrichedEvent> alertingPipeline(StreamsBuilder builder, AlertingTopology topology) {
        return topology.build(builder);
    }
}
