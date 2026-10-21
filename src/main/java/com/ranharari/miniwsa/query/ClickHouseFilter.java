package com.ranharari.miniwsa.query;

import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** The part of the WHERE clause every query shares: the time range and an optional configId. */
final class ClickHouseFilter {

    /** Same UTC text format the writer uses, so the bounds are read as UTC whatever the server's time zone. */
    private static final DateTimeFormatter CLICKHOUSE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private ClickHouseFilter() {
    }

    /** The configId part is a fixed string; its value is always a bound parameter. */
    static String where(Long configId) {
        String where = "timestamp >= toDateTime64(:from, 3, 'UTC') AND timestamp < toDateTime64(:to, 3, 'UTC')";
        return configId != null ? where + " AND configId = :configId" : where;
    }

    /** Binds the parameters that {@link #where} uses. */
    static JdbcClient.StatementSpec bind(JdbcClient.StatementSpec spec, Long configId, TimeRange range) {
        JdbcClient.StatementSpec bound = spec
                .param("from", CLICKHOUSE_TIME.format(range.from()))
                .param("to", CLICKHOUSE_TIME.format(range.to()));
        return configId != null ? bound.param("configId", configId) : bound;
    }
}
