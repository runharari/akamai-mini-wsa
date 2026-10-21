package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.Rule;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.common.model.Severity;
import com.ranharari.miniwsa.query.StatsSummary.AttackerStats;
import com.ranharari.miniwsa.query.StatsSummary.CategoryStats;
import com.ranharari.miniwsa.query.StatsSummary.PathStats;
import com.ranharari.miniwsa.sink.ClickHouseEventWriter;
import com.ranharari.miniwsa.support.ClickHouseTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** The five queries against a real ClickHouse with a small known data set. */
class ClickHouseStatsRepositoryIT {

    private static final Instant FROM = Instant.parse("2026-05-20T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-05-21T00:00:00Z");
    private static final Instant NOON = Instant.parse("2026-05-20T12:00:00Z");
    private static final TimeRange DAY = new TimeRange(FROM, TO);

    private final JdbcTemplate jdbc = ClickHouseTestSupport.jdbcTemplate();
    private final ClickHouseEventWriter writer = new ClickHouseEventWriter(jdbc);
    private final ClickHouseStatsRepository repository = new ClickHouseStatsRepository(JdbcClient.create(jdbc));
    private final AtomicInteger ids = new AtomicInteger();

    @BeforeEach
    void cleanTable() {
        ClickHouseTestSupport.truncate(jdbc);
    }

    @Test
    void countsEventsInTheRange() {
        insert(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));
        insert(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));
        insert(event(2L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));

        assertThat(repository.totalEvents(null, DAY)).isEqualTo(3);
    }

    @Test
    void filtersByConfigIdAndCountsAllConfigsWhenNull() {
        insert(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/one", 10));
        insert(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/one", 10));
        insert(event(2L, NOON, Category.INJECTION, Action.ALERT, "2.2.2.2", "/two", 90));

        assertThat(repository.totalEvents(1L, DAY)).isEqualTo(2);
        assertThat(repository.byCategory(1L, DAY)).containsOnlyKeys("BOT");
        assertThat(repository.byAction(1L, DAY)).containsOnlyKeys("DENY");
        assertThat(repository.topAttackers(1L, DAY)).extracting(AttackerStats::clientIp).containsExactly("1.1.1.1");
        assertThat(repository.topTargetedPaths(1L, DAY)).extracting(PathStats::path).containsExactly("/one");

        assertThat(repository.totalEvents(null, DAY)).isEqualTo(3);
        assertThat(repository.byCategory(null, DAY)).containsOnlyKeys("BOT", "INJECTION");
    }

    @Test
    void includesFromAndExcludesTo() {
        insert(event(1L, FROM.minusMillis(1), Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));
        insert(event(1L, FROM, Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));
        insert(event(1L, TO.minusMillis(1), Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));
        insert(event(1L, TO, Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));

        assertThat(repository.totalEvents(null, DAY)).isEqualTo(2);
    }

    @Test
    void groupsByCategoryWithRoundedAverageAndBiggestFirst() {
        insert(event(1L, NOON, Category.INJECTION, Action.DENY, "1.1.1.1", "/a", 70));
        insert(event(1L, NOON, Category.INJECTION, Action.DENY, "1.1.1.1", "/a", 75));
        insert(event(1L, NOON, Category.BOT, Action.ALERT, "1.1.1.1", "/a", 10));
        insert(event(1L, NOON, Category.BOT, Action.ALERT, "1.1.1.1", "/a", 10));
        insert(event(1L, NOON, Category.BOT, Action.ALERT, "1.1.1.1", "/a", 11));
        insert(event(1L, NOON, Category.XSS, Action.MONITOR, "1.1.1.1", "/a", 50));
        insert(event(1L, NOON, Category.DOS, Action.MONITOR, "1.1.1.1", "/a", 50));

        assertThat(repository.byCategory(null, DAY)).containsExactly(
                java.util.Map.entry("BOT", new CategoryStats(3, 10.3)),
                java.util.Map.entry("INJECTION", new CategoryStats(2, 72.5)),
                java.util.Map.entry("DOS", new CategoryStats(1, 50.0)),
                java.util.Map.entry("XSS", new CategoryStats(1, 50.0)));
    }

    @Test
    void groupsByActionBiggestFirst() {
        insert(event(1L, NOON, Category.BOT, Action.ALERT, "1.1.1.1", "/a", 10));
        insert(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));
        insert(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));

        assertThat(repository.byAction(null, DAY)).containsExactly(
                java.util.Map.entry("DENY", 2L), java.util.Map.entry("ALERT", 1L));
    }

    @Test
    void ranksAttackersByCountThenIpAndKeepsTen() {
        // 11 IPs with one event each, plus 9.9.9.9 with three events and 5.5.5.5 with two.
        List<EnrichedEvent> events = new ArrayList<>();
        for (int i = 1; i <= 11; i++) {
            events.add(event(1L, NOON, Category.BOT, Action.DENY, "10.0.0." + i, "/a", 10));
        }
        for (int i = 0; i < 3; i++) {
            events.add(event(1L, NOON, Category.BOT, Action.DENY, "9.9.9.9", "/a", i == 0 ? 20 : 21));
        }
        for (int i = 0; i < 2; i++) {
            events.add(event(1L, NOON, Category.BOT, Action.DENY, "5.5.5.5", "/a", 30));
        }
        insert(events.toArray(EnrichedEvent[]::new));

        List<AttackerStats> top = repository.topAttackers(null, DAY);

        assertThat(top).hasSize(10);
        assertThat(top.get(0)).isEqualTo(new AttackerStats("9.9.9.9", 3, 20.7));
        assertThat(top.get(1)).isEqualTo(new AttackerStats("5.5.5.5", 2, 30.0));
        // Ties are ordered by IP as text: 10.0.0.1, 10.0.0.10, 10.0.0.11, 10.0.0.2 ...
        assertThat(top.subList(2, 10)).extracting(AttackerStats::clientIp).containsExactly(
                "10.0.0.1", "10.0.0.10", "10.0.0.11", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6");
    }

    @Test
    void ranksPathsByCountThenPathAndKeepsTen() {
        List<EnrichedEvent> events = new ArrayList<>();
        for (int i = 1; i <= 11; i++) {
            events.add(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/p" + (char) ('a' + i), 10));
        }
        for (int i = 0; i < 4; i++) {
            events.add(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/login", 10));
        }
        insert(events.toArray(EnrichedEvent[]::new));

        List<PathStats> top = repository.topTargetedPaths(null, DAY);

        assertThat(top).hasSize(10);
        assertThat(top.get(0)).isEqualTo(new PathStats("/login", 4));
        assertThat(top.get(1)).isEqualTo(new PathStats("/pb", 1));
        assertThat(top.get(9)).isEqualTo(new PathStats("/pj", 1));
    }

    @Test
    void returnsZeroAndEmptyResultsForAnEmptyRange() {
        insert(event(1L, NOON, Category.BOT, Action.DENY, "1.1.1.1", "/a", 10));
        TimeRange other = new TimeRange(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-02T00:00:00Z"));

        assertThat(repository.totalEvents(null, other)).isZero();
        assertThat(repository.byCategory(null, other)).isEmpty();
        assertThat(repository.byAction(null, other)).isEmpty();
        assertThat(repository.topAttackers(null, other)).isEmpty();
        assertThat(repository.topTargetedPaths(null, other)).isEmpty();
    }

    private void insert(EnrichedEvent... events) {
        writer.insert(List.of(events));
    }

    private EnrichedEvent event(long configId, Instant timestamp, Category category, Action action,
                                String clientIp, String path, int threatScore) {
        Rule rule = new Rule("1", "RULE", "message", Severity.HIGH, category);
        SecurityEvent event = new SecurityEvent("evt-" + ids.incrementAndGet(), timestamp, configId, "pol",
                clientIp, "www.example.com", path, "GET", 403, "agent", rule, action, null, null, null, NOON);
        return new EnrichedEvent(event, "attack", threatScore);
    }
}
