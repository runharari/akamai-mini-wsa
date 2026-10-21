package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;

import java.time.Instant;

public interface TimeRangeResolver {

    /**
     * Fills in missing bounds: no {@code to} means now, no {@code from} means 24 hours before {@code to}.
     *
     * @throws InvalidRequestException if the resulting {@code from} is not before {@code to}
     */
    TimeRange resolve(Instant from, Instant to);
}
