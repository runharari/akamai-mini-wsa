package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.Rule;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.common.model.Severity;
import com.ranharari.miniwsa.support.ClickHouseTestSupport;
import com.ranharari.miniwsa.support.TestEvents;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The insert against a real ClickHouse: column mapping, NULLs and UTC times. */
class ClickHouseEventWriterIT {

    private final JdbcTemplate jdbc = ClickHouseTestSupport.jdbcTemplate();
    private final ClickHouseEventWriter writer = new ClickHouseEventWriter(jdbc);

    @BeforeEach
    void cleanTable() {
        ClickHouseTestSupport.truncate(jdbc);
    }

    @Test
    void insertsBatchWithAllFields() {
        writer.insert(List.of(TestEvents.enriched("a"), TestEvents.enriched("b")));

        Map<String, Object> row = jdbc.queryForMap("""
                SELECT eventId, toString(timestamp) AS ts, toString(receivedAt) AS received, configId, clientIp,
                       statusCode, severity, category, action, country, city, requestSize, attackType, threatScore
                FROM events WHERE eventId = 'a'""");

        assertThat(jdbc.queryForObject("SELECT count() FROM events", Long.class)).isEqualTo(2);
        assertThat(row.get("ts")).isEqualTo("2026-05-20 14:32:10.000");
        assertThat(row.get("received")).isEqualTo("2026-05-20 14:32:11.000");
        assertThat(((Number) row.get("configId")).longValue()).isEqualTo(14227);
        assertThat(row.get("clientIp")).isEqualTo("203.0.113.42");
        assertThat(((Number) row.get("statusCode")).intValue()).isEqualTo(403);
        assertThat(row.get("severity")).isEqualTo("CRITICAL");
        assertThat(row.get("category")).isEqualTo("INJECTION");
        assertThat(row.get("action")).isEqualTo("DENY");
        assertThat(row.get("country")).isEqualTo("CN");
        assertThat(row.get("attackType")).isEqualTo("SQL/Command Injection");
        assertThat(((Number) row.get("threatScore")).intValue()).isEqualTo(75);
    }

    @Test
    void storesMissingOptionalNumbersAsNullAndTextAsEmpty() {
        SecurityEvent minimal = new SecurityEvent("min", Instant.parse("2026-05-20T14:32:10Z"), 1L, null,
                "1.2.3.4", null, "/", "GET", null, null, new Rule("1", null, null, Severity.LOW, Category.BOT),
                Action.MONITOR, null, null, null, Instant.parse("2026-05-20T14:32:11Z"));

        writer.insert(List.of(new EnrichedEvent(minimal, "Bot Activity", 10)));

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT statusCode, requestSize, responseSize, policyId, country, userAgent FROM events");
        assertThat(row.get("statusCode")).isNull();
        assertThat(row.get("requestSize")).isNull();
        assertThat(row.get("responseSize")).isNull();
        assertThat(row.get("policyId")).isEqualTo("");
        assertThat(row.get("country")).isEqualTo("");
        assertThat(row.get("userAgent")).isEqualTo("");
    }
}
