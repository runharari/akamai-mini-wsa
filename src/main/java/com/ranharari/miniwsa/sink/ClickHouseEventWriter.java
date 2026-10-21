package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.GeoLocation;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Inserts a batch with one JDBC batch statement, which the ClickHouse driver sends as a single
 * INSERT: ClickHouse wants few large inserts, not many small ones.
 */
@Repository
@Profile({"sink", "default"})
public class ClickHouseEventWriter implements EventWriter {

    /**
     * The table has one partition per day, and by default ClickHouse refuses an insert that touches
     * more than 100 of them. Ingest accepts events up to a year old, so one batch may span more days:
     * the setting turns that refusal into a warning instead of a failed (and endlessly retried) batch.
     */
    private static final String INSERT = """
            INSERT INTO events (eventId, timestamp, receivedAt, configId, policyId, clientIp, hostname, path,
                                method, statusCode, userAgent, ruleId, ruleName, ruleMessage, severity, category,
                                action, country, city, requestSize, responseSize, attackType, threatScore)
            SETTINGS throw_on_max_partitions_per_insert_block = 0
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    /**
     * Times are sent as UTC text ("2026-05-20 14:32:10.000"); the columns are DateTime64(3, 'UTC'),
     * so ClickHouse reads them as UTC no matter what time zone the JVM or the server is in.
     */
    private static final DateTimeFormatter CLICKHOUSE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private final JdbcTemplate jdbcTemplate;

    public ClickHouseEventWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void insert(List<EnrichedEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        try {
            jdbcTemplate.batchUpdate(INSERT, events, events.size(), ClickHouseEventWriter::setRow);
        } catch (DataAccessException e) {
            throw new StorageException("Insert of " + events.size() + " events into ClickHouse failed", e);
        }
    }

    @Override
    public boolean isAvailable() {
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            return true;
        } catch (DataAccessException e) {
            return false;
        }
    }

    private static void setRow(PreparedStatement ps, EnrichedEvent enriched) throws SQLException {
        SecurityEvent e = enriched.event();
        GeoLocation geo = e.geoLocation();
        int i = 1;
        ps.setString(i++, e.eventId());
        ps.setString(i++, CLICKHOUSE_TIME.format(e.timestamp()));
        ps.setString(i++, CLICKHOUSE_TIME.format(e.receivedAt()));
        ps.setLong(i++, e.configId());
        ps.setString(i++, text(e.policyId()));
        ps.setString(i++, e.clientIp());
        ps.setString(i++, text(e.hostname()));
        ps.setString(i++, e.path());
        ps.setString(i++, e.method());
        setNullableInt(ps, i++, e.statusCode());
        ps.setString(i++, text(e.userAgent()));
        ps.setString(i++, e.rule().id());
        ps.setString(i++, text(e.rule().name()));
        ps.setString(i++, text(e.rule().message()));
        ps.setString(i++, e.rule().severity().name());
        ps.setString(i++, e.rule().category().name());
        ps.setString(i++, e.action().name());
        ps.setString(i++, geo != null ? text(geo.country()) : "");
        ps.setString(i++, geo != null ? text(geo.city()) : "");
        setNullableLong(ps, i++, e.requestSize());
        setNullableLong(ps, i++, e.responseSize());
        ps.setString(i++, text(enriched.attackType()));
        ps.setInt(i, enriched.threatScore());
    }

    /** Optional text fields are stored as '' rather than NULL (cheaper in ClickHouse). */
    private static String text(String value) {
        return value != null ? value : "";
    }

    private static void setNullableInt(PreparedStatement ps, int index, Integer value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.INTEGER);
        } else {
            ps.setInt(index, value);
        }
    }

    private static void setNullableLong(PreparedStatement ps, int index, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.BIGINT);
        } else {
            ps.setLong(index, value);
        }
    }
}
