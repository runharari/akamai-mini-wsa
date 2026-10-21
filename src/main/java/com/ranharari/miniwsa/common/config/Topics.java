package com.ranharari.miniwsa.common.config;

/** Kafka topic names shared by the modules. */
public final class Topics {

    /** Validated events from the ingest API, keyed by clientIp. */
    public static final String RAW = "events.raw";

    /** Events after classification and scoring, read by the ClickHouse sink. */
    public static final String ENRICHED = "events.enriched";

    private Topics() {
    }
}
