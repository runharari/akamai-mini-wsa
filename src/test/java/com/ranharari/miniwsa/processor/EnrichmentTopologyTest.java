package com.ranharari.miniwsa.processor;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.metrics.MicrometerMetricsFactory;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.enrich.DefaultAttackClassifier;
import com.ranharari.miniwsa.enrich.DefaultEnricher;
import com.ranharari.miniwsa.enrich.DefaultRepeatOffenderWindow;
import com.ranharari.miniwsa.enrich.DefaultThreatScorer;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow.History;
import com.ranharari.miniwsa.processor.EnrichmentTopology.TopologySerdes;
import com.ranharari.miniwsa.support.TestEvents;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the real topology (real scorer, classifier, window and state store) in memory: no broker. */
class EnrichmentTopologyTest {

    private static final Instant T0 = Instant.parse("2026-05-20T14:00:00Z");
    private static final String ATTACKER = "203.0.113.42";

    private final JsonMapper jsonMapper = JsonMapper.builder().build();
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    private TopologyTestDriver driver;
    private TestInputTopic<String, SecurityEvent> raw;
    private TestOutputTopic<String, EnrichedEvent> enriched;

    @BeforeEach
    void setUp() {
        TopologySerdes serdes = new TopologySerdes(serde(SecurityEvent.class), serde(EnrichedEvent.class), serde(History.class));
        EnrichmentTopology topology = new EnrichmentTopology(serdes, new DefaultRepeatOffenderWindow(),
                new DefaultEnricher(new DefaultAttackClassifier(), new DefaultThreatScorer()),
                new MicrometerMetricsFactory(meters));

        StreamsBuilder builder = new StreamsBuilder();
        topology.build(builder);
        driver = new TopologyTestDriver(builder.build());
        raw = driver.createInputTopic(Topics.RAW, Serdes.String().serializer(), serdes.raw().serializer());
        enriched = driver.createOutputTopic(Topics.ENRICHED, Serdes.String().deserializer(), serdes.enriched().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void addsAttackTypeAndScoreAndKeepsTheIpKey() {
        // CRITICAL 40 + DENY 20 + "/api/v1/login" 15
        send(TestEvents.event("e1", ATTACKER, T0));

        KeyValue<String, EnrichedEvent> out = enriched.readKeyValue();
        assertThat(out.key).isEqualTo(ATTACKER);
        assertThat(out.value.attackType()).isEqualTo("SQL/Command Injection");
        assertThat(out.value.threatScore()).isEqualTo(75);
    }

    @Test
    void seventhEventFromTheSameIpWithinTenMinutesGetsTheRepeatOffenderBonus() {
        for (int i = 0; i < 7; i++) {
            send(TestEvents.event("e" + i, ATTACKER, T0.plusSeconds(30L * i)));
        }

        List<Integer> scores = enriched.readValuesToList().stream().map(EnrichedEvent::threatScore).toList();
        assertThat(scores).containsExactly(75, 75, 75, 75, 75, 75, 90);
        assertThat(meters.get("wsa.enrich.events").counter().count()).isEqualTo(7);
        assertThat(meters.get("wsa.enrich.repeat_offenders").counter().count()).isEqualTo(1);
    }

    @Test
    void usesEventTimeSoEventsHoursApartAreNotRepeatOffenders() {
        for (int i = 0; i < 7; i++) {
            send(TestEvents.event("e" + i, ATTACKER, T0.plusSeconds(3600L * i)));
        }

        assertThat(enriched.readValuesToList()).extracting(EnrichedEvent::threatScore).containsOnly(75);
        assertThat(meters.get("wsa.enrich.repeat_offenders").counter().count()).isZero();
    }

    @Test
    void forgetsIpsThatWereIdleForMoreThanTenMinutesOfEventTime() {
        KeyValueStore<String, History> store = driver.getKeyValueStore(EnrichProcessor.STORE);
        send(TestEvents.event("old", ATTACKER, T0));
        assertThat(store.get(ATTACKER)).isNotNull();

        // Another IP's event 15 minutes later moves stream time forward and triggers the eviction.
        send(TestEvents.event("other", "198.51.100.7", T0.plusSeconds(15 * 60)));

        assertThat(store.get(ATTACKER)).isNull();
        assertThat(store.get("198.51.100.7")).isNotNull();
    }

    private void send(SecurityEvent event) {
        raw.pipeInput(event.clientIp(), event);
    }

    private <T> JacksonJsonSerde<T> serde(Class<T> type) {
        return new JacksonJsonSerde<>(type, jsonMapper).noTypeInfo().ignoreTypeHeaders();
    }
}
