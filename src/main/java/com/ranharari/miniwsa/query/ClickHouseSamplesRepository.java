package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.GeoLocation;
import com.ranharari.miniwsa.common.model.Rule;
import com.ranharari.miniwsa.common.model.Severity;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

/**
 * Reads single events. No FINAL: a batch re-delivered by the sink may show an event twice until
 * ClickHouse merges the duplicates in the background; FINAL would make every query slower.
 */
@Repository
@Profile({"query", "default"})
public class ClickHouseSamplesRepository implements SamplesRepository {

    /** Times are selected as epoch milliseconds, so no JVM or server time zone can shift them. */
    private static final String COLUMNS = """
            eventId, toUnixTimestamp64Milli(timestamp) AS timestampMs, toUnixTimestamp64Milli(receivedAt) AS receivedAtMs,
            configId, policyId, clientIp, hostname, path, method, statusCode, userAgent,
            ruleId, ruleName, ruleMessage, severity, category, action, country, city,
            requestSize, responseSize, attackType, threatScore""";

    private final JdbcClient jdbc;

    public ClickHouseSamplesRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public long count(SampleFilter filter) {
        String sql = "SELECT count() FROM events WHERE " + where(filter);
        return query("count samples", () -> bind(sql, filter).query(Long.class).single());
    }

    @Override
    public List<EventSample> find(SampleFilter filter, int limit, int offset) {
        String sql = "SELECT " + COLUMNS + " FROM events WHERE " + where(filter)
                + " ORDER BY timestamp DESC, eventId LIMIT :limit OFFSET :offset";
        return query("find samples", () -> bind(sql, filter)
                .param("limit", limit)
                .param("offset", offset)
                .query((rs, i) -> toSample(rs))
                .list());
    }

    /** The shared filter. Only fixed fragments are added here; the values are always bound parameters. */
    private static String where(SampleFilter filter) {
        String where = ClickHouseFilter.where(filter.configId());
        if (filter.category() != null) {
            where += " AND category = :category";
        }
        if (filter.action() != null) {
            where += " AND action = :action";
        }
        return where;
    }

    private JdbcClient.StatementSpec bind(String sql, SampleFilter filter) {
        JdbcClient.StatementSpec spec = ClickHouseFilter.bind(jdbc.sql(sql), filter.configId(), filter.range());
        if (filter.category() != null) {
            spec = spec.param("category", filter.category().name());
        }
        if (filter.action() != null) {
            spec = spec.param("action", filter.action().name());
        }
        return spec;
    }

    private static EventSample toSample(ResultSet rs) throws SQLException {
        Rule rule = new Rule(rs.getString("ruleId"), textOrNull(rs, "ruleName"), textOrNull(rs, "ruleMessage"),
                Severity.valueOf(rs.getString("severity")), Category.valueOf(rs.getString("category")));
        return new EventSample(
                rs.getString("eventId"),
                Instant.ofEpochMilli(rs.getLong("timestampMs")),
                Instant.ofEpochMilli(rs.getLong("receivedAtMs")),
                rs.getLong("configId"),
                textOrNull(rs, "policyId"),
                rs.getString("clientIp"),
                textOrNull(rs, "hostname"),
                rs.getString("path"),
                rs.getString("method"),
                intOrNull(rs, "statusCode"),
                textOrNull(rs, "userAgent"),
                rule,
                Action.valueOf(rs.getString("action")),
                geo(rs),
                longOrNull(rs, "requestSize"),
                longOrNull(rs, "responseSize"),
                rs.getString("attackType"),
                rs.getInt("threatScore"));
    }

    /** No location is stored as two empty strings. */
    private static GeoLocation geo(ResultSet rs) throws SQLException {
        String country = rs.getString("country");
        String city = rs.getString("city");
        return country.isEmpty() && city.isEmpty() ? null : new GeoLocation(country, city);
    }

    /** Optional text is stored as ''; the API shows it as null. */
    private static String textOrNull(ResultSet rs, String column) throws SQLException {
        String value = rs.getString(column);
        return value.isEmpty() ? null : value;
    }

    private static Integer intOrNull(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private static Long longOrNull(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    /** Wraps a failed query once, keeping the cause. */
    private static <T> T query(String what, Supplier<T> run) {
        try {
            return run.get();
        } catch (DataAccessException e) {
            throw new StorageException("ClickHouse query failed: " + what, e);
        }
    }
}
