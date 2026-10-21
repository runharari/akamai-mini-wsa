package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.StorageException;

import java.util.List;

/** Both methods throw {@link StorageException} if the query fails. */
public interface SamplesRepository {

    /** All events that match the filter. */
    long count(SampleFilter filter);

    /** One page of the matching events, newest first; events with the same timestamp are ordered by eventId. */
    List<EventSample> find(SampleFilter filter, int limit, int offset);
}
