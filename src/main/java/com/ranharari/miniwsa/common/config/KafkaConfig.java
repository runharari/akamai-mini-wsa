package com.ranharari.miniwsa.common.config;

import com.ranharari.miniwsa.common.model.EnrichedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

import java.util.function.Supplier;

/**
 * Topics and JSON (de)serialization shared by every role.
 * We hand Kafka the same JsonMapper Spring MVC uses, so an Instant is written the same way everywhere.
 */
@Configuration
public class KafkaConfig {

    /** One local broker, so no replication. Production would use 3. */
    private static final int REPLICAS = 1;

    @Bean
    NewTopic rawTopic(KafkaTopicProperties properties) {
        return TopicBuilder.name(Topics.RAW).partitions(properties.partitions()).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic enrichedTopic(KafkaTopicProperties properties) {
        return TopicBuilder.name(Topics.ENRICHED).partitions(properties.partitions()).replicas(REPLICAS).build();
    }

    @Bean
    DefaultKafkaProducerFactoryCustomizer jsonValueSerializer(JsonMapper jsonMapper) {
        // No type headers: consumers know the type of each topic.
        return factory -> valueSerializer(factory, () -> new JacksonJsonSerializer<>(jsonMapper).noTypeInfo());
    }

    @Bean
    DefaultKafkaConsumerFactoryCustomizer jsonValueDeserializer(JsonMapper jsonMapper) {
        // Only the sink consumes with a listener, and it reads EnrichedEvent.
        // ErrorHandlingDeserializer turns an unreadable message into a null value instead of an
        // endless failure loop; the listener counts and skips it.
        return factory -> valueDeserializer(factory, () -> new ErrorHandlingDeserializer<>(
                new JacksonJsonDeserializer<>(EnrichedEvent.class, jsonMapper, false)));
    }

    // Boot hands the customizers wildcard-typed factories, so we cast to set the value type.
    @SuppressWarnings("unchecked")
    private static <V> void valueSerializer(DefaultKafkaProducerFactory<?, ?> factory, Supplier<Serializer<V>> serializer) {
        ((DefaultKafkaProducerFactory<Object, V>) factory).setValueSerializerSupplier(serializer);
    }

    @SuppressWarnings("unchecked")
    private static <V> void valueDeserializer(DefaultKafkaConsumerFactory<?, ?> factory, Supplier<Deserializer<V>> deserializer) {
        ((DefaultKafkaConsumerFactory<Object, V>) factory).setValueDeserializerSupplier(deserializer);
    }
}
