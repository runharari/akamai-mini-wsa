package com.ranharari.miniwsa.common.config;

import com.ranharari.miniwsa.common.model.EnrichedEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

import java.util.function.Supplier;

/**
 * How Kafka messages are written and read: JSON, using the same JsonMapper as Spring MVC,
 * so an Instant is written the same way everywhere.
 */
@Configuration
public class KafkaSerializationConfig {

    @Bean
    DefaultKafkaProducerFactoryCustomizer jsonValueSerializer(JsonMapper jsonMapper) {
        // No type headers: consumers know the type of each topic.
        return factory -> valueSerializer(factory, () -> new JacksonJsonSerializer<>(jsonMapper).noTypeInfo());
    }

    @Bean
    DefaultKafkaConsumerFactoryCustomizer jsonValueDeserializer(JsonMapper jsonMapper) {
        // Only the sink consumes with a Spring Kafka listener, and it reads EnrichedEvent.
        // ErrorHandlingDeserializer turns an unreadable message into a null value instead of an
        // endless failure loop; EventSink counts and skips it.
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
