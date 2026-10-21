package com.ranharari.miniwsa.processor;

import com.ranharari.miniwsa.common.event.EnrichedEvent;
import com.ranharari.miniwsa.common.event.SecurityEvent;
import com.ranharari.miniwsa.common.kafka.Topics;
import com.ranharari.miniwsa.support.TestEvents;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the real topology in memory: no Kafka broker. */
class EnrichmentTopologyTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final JacksonJsonSerde<SecurityEvent> rawSerde =
            new JacksonJsonSerde<>(SecurityEvent.class, jsonMapper).noTypeInfo().ignoreTypeHeaders();
    private final JacksonJsonSerde<EnrichedEvent> enrichedSerde =
            new JacksonJsonSerde<>(EnrichedEvent.class, jsonMapper).noTypeInfo().ignoreTypeHeaders();

    private TopologyTestDriver driver;
    private TestInputTopic<String, SecurityEvent> raw;
    private TestOutputTopic<String, EnrichedEvent> enriched;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new EnrichmentTopology(rawSerde, enrichedSerde).build(builder);
        driver = new TopologyTestDriver(builder.build());
        raw = driver.createInputTopic(Topics.RAW, Serdes.String().serializer(), rawSerde.serializer());
        enriched = driver.createOutputTopic(Topics.ENRICHED, Serdes.String().deserializer(), enrichedSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void forwardsEachEventToEnrichedTopicKeepingTheIpKey() {
        SecurityEvent event = TestEvents.event("a", "1.1.1.1", Instant.parse("2026-05-20T14:32:10Z"));

        raw.pipeInput("1.1.1.1", event);

        KeyValue<String, EnrichedEvent> out = enriched.readKeyValue();
        assertThat(out.key).isEqualTo("1.1.1.1");
        assertThat(out.value.event()).isEqualTo(event);
        assertThat(enriched.isEmpty()).isTrue();
    }
}
