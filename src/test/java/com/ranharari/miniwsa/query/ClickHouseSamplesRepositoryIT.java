package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.GeoLocation;
import com.ranharari.miniwsa.common.model.Rule;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.common.model.Severity;
import com.ranharari.miniwsa.sink.ClickHouseEventWriter;
import com.ranharari.miniwsa.support.ClickHouseTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Count and find against a real ClickHouse with a small known data set. */
class ClickHouseSamplesRepositoryIT {

    private static final Instant FROM = Instant.parse("2026-05-20T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-05-21T00:00:00Z");
    private static final Instant NOON = Instant.parse("2026-05-20T12:00:00Z");
    private static final TimeRange DAY = new TimeRange(FROM, TO);
    private static final SampleFilter ALL = new SampleFilter(null, DAY, null, null);

    private final JdbcTemplate jdbc = ClickHouseTestSupport.jdbcTemplate();
    private final ClickHouseEventWriter writer = new ClickHouseEventWriter(jdbc);
    private final ClickHouseSamplesRepository repository = new ClickHouseSamplesRepository(JdbcClient.create(jdbc));

    @BeforeEach
    void cleanTable() {
        ClickHouseTestSupport.truncate(jdbc);
    }

    @Test
    void returnsNewestFirst() {
        insert(event("a", 1L, NOON.minusSeconds(10), Category.BOT, Action.DENY));
        insert(event("b", 1L, NOON, Category.BOT, Action.DENY));
        insert(event("c", 1L, NOON.minusSeconds(5), Category.BOT, Action.DENY));

        assertThat(ids(repository.find(ALL, 10, 0))).containsExactly("b", "c", "a");
    }

    @Test
    void ordersEqualTimestampsByEventId() {
        insert(event("evt-3", 1L, NOON, Category.BOT, Action.DENY));
        insert(event("evt-1", 1L, NOON, Category.BOT, Action.DENY));
        insert(event("evt-2", 1L, NOON, Category.BOT, Action.DENY));

        assertThat(ids(repository.find(ALL, 10, 0))).containsExactly("evt-1", "evt-2", "evt-3");
    }

    @Test
    void filtersByConfigId() {
        insert(event("a", 1L, NOON, Category.BOT, Action.DENY));
        insert(event("b", 2L, NOON, Category.BOT, Action.DENY));

        SampleFilter filter = new SampleFilter(2L, DAY, null, null);

        assertThat(ids(repository.find(filter, 10, 0))).containsExactly("b");
        assertThat(repository.count(filter)).isEqualTo(1);
    }

    @Test
    void filtersByCategory() {
        insert(event("a", 1L, NOON, Category.BOT, Action.DENY));
        insert(event("b", 1L, NOON, Category.XSS, Action.DENY));

        SampleFilter filter = new SampleFilter(null, DAY, Category.XSS, null);

        assertThat(ids(repository.find(filter, 10, 0))).containsExactly("b");
        assertThat(repository.count(filter)).isEqualTo(1);
    }

    @Test
    void filtersByAction() {
        insert(event("a", 1L, NOON, Category.BOT, Action.DENY));
        insert(event("b", 1L, NOON, Category.BOT, Action.ALERT));

        SampleFilter filter = new SampleFilter(null, DAY, null, Action.ALERT);

        assertThat(ids(repository.find(filter, 10, 0))).containsExactly("b");
        assertThat(repository.count(filter)).isEqualTo(1);
    }

    @Test
    void combinesAllFilters() {
        insert(event("match", 1L, NOON, Category.BOT, Action.DENY));
        insert(event("other-config", 2L, NOON, Category.BOT, Action.DENY));
        insert(event("other-category", 1L, NOON, Category.XSS, Action.DENY));
        insert(event("other-action", 1L, NOON, Category.BOT, Action.ALERT));

        SampleFilter filter = new SampleFilter(1L, DAY, Category.BOT, Action.DENY);

        assertThat(ids(repository.find(filter, 10, 0))).containsExactly("match");
        assertThat(repository.count(filter)).isEqualTo(1);
    }

    @Test
    void includesFromAndExcludesTo() {
        insert(event("before", 1L, FROM.minusMillis(1), Category.BOT, Action.DENY));
        insert(event("at-from", 1L, FROM, Category.BOT, Action.DENY));
        insert(event("last", 1L, TO.minusMillis(1), Category.BOT, Action.DENY));
        insert(event("at-to", 1L, TO, Category.BOT, Action.DENY));

        assertThat(ids(repository.find(ALL, 10, 0))).containsExactly("last", "at-from");
        assertThat(repository.count(ALL)).isEqualTo(2);
    }

    @Test
    void pagesWhileCountStillReturnsAllMatches() {
        for (int i = 1; i <= 5; i++) {
            insert(event("evt-" + i, 1L, NOON.minusSeconds(i), Category.BOT, Action.DENY));
        }

        assertThat(ids(repository.find(ALL, 2, 2))).containsExactly("evt-3", "evt-4");
        assertThat(repository.count(ALL)).isEqualTo(5);
    }

    @Test
    void returnsAFullEventWithEveryFieldEqual() {
        SecurityEvent input = new SecurityEvent("evt-00132", Instant.parse("2026-05-20T14:32:10.123Z"), 14227L,
                "pol_web1", "203.0.113.42", "www.example.com", "/api/v1/login", "POST", 403, "Mozilla/5.0",
                new Rule("950001", "SQL_INJECTION", "SQL Injection Attack Detected", Severity.CRITICAL,
                        Category.INJECTION),
                Action.DENY, new GeoLocation("CN", "Beijing"), 1024L, 256L,
                Instant.parse("2026-05-20T14:32:11.456Z"));
        insert(new EnrichedEvent(input, "SQL/Command Injection", 75));

        List<EventSample> found = repository.find(ALL, 10, 0);

        assertThat(found).containsExactly(new EventSample("evt-00132", Instant.parse("2026-05-20T14:32:10.123Z"),
                Instant.parse("2026-05-20T14:32:11.456Z"), 14227L, "pol_web1", "203.0.113.42", "www.example.com",
                "/api/v1/login", "POST", 403, "Mozilla/5.0",
                new Rule("950001", "SQL_INJECTION", "SQL Injection Attack Detected", Severity.CRITICAL,
                        Category.INJECTION),
                Action.DENY, new GeoLocation("CN", "Beijing"), 1024L, 256L, "SQL/Command Injection", 75));
    }

    @Test
    void returnsNullsForMissingOptionalFields() {
        SecurityEvent input = new SecurityEvent("evt-min", NOON, 1L, null, "1.1.1.1", null, "/a", "GET", null, null,
                new Rule("1", null, null, Severity.LOW, Category.BOT), Action.MONITOR, null, null, null, NOON);
        insert(new EnrichedEvent(input, "Bot", 10));

        EventSample sample = repository.find(ALL, 10, 0).getFirst();

        assertThat(sample.policyId()).isNull();
        assertThat(sample.hostname()).isNull();
        assertThat(sample.userAgent()).isNull();
        assertThat(sample.statusCode()).isNull();
        assertThat(sample.geoLocation()).isNull();
        assertThat(sample.requestSize()).isNull();
        assertThat(sample.responseSize()).isNull();
        assertThat(sample.rule()).isEqualTo(new Rule("1", null, null, Severity.LOW, Category.BOT));
    }

    @Test
    void returnsNothingForAnEmptyRange() {
        insert(event("a", 1L, NOON, Category.BOT, Action.DENY));
        SampleFilter other = new SampleFilter(null,
                new TimeRange(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-02T00:00:00Z")), null, null);

        assertThat(repository.find(other, 10, 0)).isEmpty();
        assertThat(repository.count(other)).isZero();
    }

    private void insert(EnrichedEvent... events) {
        writer.insert(List.of(events));
    }

    private static List<String> ids(List<EventSample> samples) {
        return samples.stream().map(EventSample::eventId).toList();
    }

    private static EnrichedEvent event(String eventId, long configId, Instant timestamp, Category category,
                                       Action action) {
        Rule rule = new Rule("1", "RULE", "message", Severity.HIGH, category);
        SecurityEvent event = new SecurityEvent(eventId, timestamp, configId, "pol", "1.1.1.1", "www.example.com",
                "/a", "GET", 403, "agent", rule, action, null, null, null, NOON);
        return new EnrichedEvent(event, "attack", 10);
    }
}
