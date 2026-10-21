package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.support.TestEvents;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the real alerting topology in memory: no broker. */
class AlertingTopologyTest {

    private static final Instant T0 = Instant.parse("2026-05-20T14:00:00Z");

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    private TopologyTestDriver driver;
    private TestInputTopic<String, EnrichedEvent> enriched;
    private TestInputTopic<String, AlertRule> rules;

    @BeforeEach
    void setUp() {
        JacksonJsonSerde<EnrichedEvent> enrichedSerde = serde(EnrichedEvent.class);
        JacksonJsonSerde<AlertRule> ruleSerde = serde(AlertRule.class);
        StreamsBuilder builder = new StreamsBuilder();
        new AlertingTopology(enrichedSerde, ruleSerde).build(builder);
        driver = new TopologyTestDriver(builder.build());
        enriched = driver.createInputTopic(Topics.ENRICHED, Serdes.String().serializer(), enrichedSerde.serializer());
        rules = driver.createInputTopic(Topics.ALERT_RULES, Serdes.String().serializer(), ruleSerde.serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void countsEventsPerMinuteOfTheirOwnTimestampForTheirConfigAndForAllConfigs() {
        // TestEvents are INJECTION events of config 14227.
        send("a", T0.plusSeconds(10));
        send("b", T0.plusSeconds(50));
        send("c", T0.plusSeconds(70)); // the next minute

        assertThat(minuteCounts("14227|INJECTION")).containsExactly(2L, 1L);
        assertThat(minuteCounts("*|INJECTION")).containsExactly(2L, 1L);
        assertThat(minuteCounts("14227|BOT")).isEmpty();
    }

    @Test
    void stillCountsEventsThatArriveHoursLate() {
        // A backfill or a second generator run sends older events after newer ones.
        send("new", T0.plusSeconds(3 * 3600));
        send("old", T0.plusSeconds(10));

        assertThat(minuteCounts("14227|INJECTION")).containsExactly(1L);
    }

    @Test
    void keepsEveryRuleInTheRulesStore() {
        AlertRule rule = new AlertRule("r1", "SQLi burst", Category.INJECTION, null, 5, 10);

        rules.pipeInput(rule.id(), rule);

        KeyValueStore<String, AlertRule> store = driver.getKeyValueStore(AlertingTopology.RULES_STORE);
        assertThat(store.get("r1")).isEqualTo(rule);
    }

    private void send(String eventId, Instant timestamp) {
        EnrichedEvent event = new EnrichedEvent(TestEvents.event(eventId, "1.2.3.4", timestamp), "SQL/Command Injection", 75);
        enriched.pipeInput("1.2.3.4", event);
    }

    private List<Long> minuteCounts(String key) {
        WindowStore<String, Long> store = driver.getWindowStore(AlertingTopology.COUNTS_STORE);
        List<Long> counts = new ArrayList<>();
        try (WindowStoreIterator<Long> minutes = store.fetch(key, T0, T0.plusSeconds(3600))) {
            minutes.forEachRemaining(minute -> counts.add(minute.value));
        }
        return counts;
    }

    private <T> JacksonJsonSerde<T> serde(Class<T> type) {
        return new JacksonJsonSerde<>(type, jsonMapper).noTypeInfo().ignoreTypeHeaders();
    }
}
