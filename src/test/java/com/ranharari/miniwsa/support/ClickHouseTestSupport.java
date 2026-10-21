package com.ranharari.miniwsa.support;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.utility.MountableFile;

/**
 * One ClickHouse container for all repository integration tests, created with the real schema.sql.
 * Started once per test JVM (singleton container) to keep the IT run short.
 */
public final class ClickHouseTestSupport {

    public static final ClickHouseContainer CLICKHOUSE = new ClickHouseContainer("clickhouse/clickhouse-server:26.3")
            .withCopyFileToContainer(MountableFile.forClasspathResource("db/schema.sql"),
                    "/docker-entrypoint-initdb.d/01-schema.sql");

    static {
        CLICKHOUSE.start();
    }

    private ClickHouseTestSupport() {
    }

    public static JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(new DriverManagerDataSource(
                CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword()));
    }

    public static void truncate(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("TRUNCATE TABLE events");
    }
}
