package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.common.model.EnrichedEvent;

import java.util.List;

/** Stores enriched events. */
public interface EventWriter {

    /**
     * Stores the whole batch in one write.
     *
     * @throws StorageException if the write failed (nothing can be assumed stored)
     */
    void insert(List<EnrichedEvent> events);
}
