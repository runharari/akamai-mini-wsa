package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.query.StatsSummary.AttackerStats;
import com.ranharari.miniwsa.query.StatsSummary.CategoryStats;
import com.ranharari.miniwsa.query.StatsSummary.PathStats;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * All aggregation happens in ClickHouse. No FINAL: a batch re-delivered by the sink may be counted
 * twice until ClickHouse merges the duplicates in the background; FINAL would make every query slower.
 */
@Repository
@Profile({"query", "default"})
public class ClickHouseStatsRepository implements StatsRepository {

    private static final int TOP_N = 10;

    /** Same UTC text format the writer uses, so the bounds are read as UTC whatever the server's time zone. */
    private static final DateTimeFormatter CLICKHOUSE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private final JdbcClient jdbc;

    public ClickHouseStatsRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public long totalEvents(Long configId, TimeRange range) {
        String sql = "SELECT count() FROM events WHERE " + where(configId);
        return query("total events", () -> bind(sql, configId, range).query(Long.class).single());
    }

    @Override
    public Map<String, CategoryStats> byCategory(Long configId, TimeRange range) {
        String sql = "SELECT category, count() AS count, round(avg(threatScore), 1) AS avgThreatScore"
                + " FROM events WHERE " + where(configId)
                + " GROUP BY category ORDER BY count DESC, category";
        return query("events by category", () -> toMap(
                bind(sql, configId, range).query((rs, i) ->
                        new Named<>(rs.getString("category"),
                                new CategoryStats(rs.getLong("count"), rs.getDouble("avgThreatScore")))).list()));
    }

    @Override
    public Map<String, Long> byAction(Long configId, TimeRange range) {
        String sql = "SELECT action, count() AS count FROM events WHERE " + where(configId)
                + " GROUP BY action ORDER BY count DESC, action";
        return query("events by action", () -> toMap(
                bind(sql, configId, range).query((rs, i) ->
                        new Named<>(rs.getString("action"), rs.getLong("count"))).list()));
    }

    @Override
    public List<AttackerStats> topAttackers(Long configId, TimeRange range) {
        String sql = "SELECT clientIp, count() AS count, round(avg(threatScore), 1) AS avgThreatScore"
                + " FROM events WHERE " + where(configId)
                + " GROUP BY clientIp ORDER BY count DESC, clientIp LIMIT " + TOP_N;
        return query("top attackers", () -> bind(sql, configId, range).query((rs, i) ->
                new AttackerStats(rs.getString("clientIp"), rs.getLong("count"), rs.getDouble("avgThreatScore")))
                .list());
    }

    @Override
    public List<PathStats> topTargetedPaths(Long configId, TimeRange range) {
        String sql = "SELECT path, count() AS count FROM events WHERE " + where(configId)
                + " GROUP BY path ORDER BY count DESC, path LIMIT " + TOP_N;
        return query("top targeted paths", () -> bind(sql, configId, range).query((rs, i) ->
                new PathStats(rs.getString("path"), rs.getLong("count"))).list());
    }

    /** The shared filter. The configId part is a fixed string; its value is always a bound parameter. */
    private static String where(Long configId) {
        String where = "timestamp >= toDateTime64(:from, 3, 'UTC') AND timestamp < toDateTime64(:to, 3, 'UTC')";
        return configId != null ? where + " AND configId = :configId" : where;
    }

    private JdbcClient.StatementSpec bind(String sql, Long configId, TimeRange range) {
        JdbcClient.StatementSpec spec = jdbc.sql(sql)
                .param("from", CLICKHOUSE_TIME.format(range.from()))
                .param("to", CLICKHOUSE_TIME.format(range.to()));
        return configId != null ? spec.param("configId", configId) : spec;
    }

    /** Keeps the SQL order. */
    private static <V> Map<String, V> toMap(List<Named<V>> rows) {
        Map<String, V> map = new LinkedHashMap<>();
        for (Named<V> row : rows) {
            map.put(row.name(), row.value());
        }
        return map;
    }

    /** Wraps a failed query once, keeping the cause. */
    private static <T> T query(String what, Supplier<T> run) {
        try {
            return run.get();
        } catch (DataAccessException e) {
            throw new StorageException("ClickHouse query failed: " + what, e);
        }
    }

    private record Named<V>(String name, V value) {
    }
}
