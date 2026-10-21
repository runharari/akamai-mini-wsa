package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.StorageException;

public interface SamplesService {

    /**
     * One page of the events that match the filter, plus the total number of matches.
     *
     * @throws StorageException if ClickHouse can't be queried
     */
    SamplesPage samples(SampleFilter filter, int limit, int offset);
}
