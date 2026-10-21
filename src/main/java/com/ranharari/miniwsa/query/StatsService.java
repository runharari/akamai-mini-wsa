package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.StorageException;

public interface StatsService {

    /**
     * Builds the summary of the events in the range; a null {@code configId} means all configs.
     *
     * @throws StorageException if ClickHouse can't be queried
     */
    StatsSummary summary(Long configId, TimeRange range);
}
