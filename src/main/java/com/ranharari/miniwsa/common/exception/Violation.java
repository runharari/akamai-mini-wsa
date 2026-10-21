package com.ranharari.miniwsa.common.exception;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * One problem in a request. {@code index} is the position of the event in an ingest batch
 * (0 for a single object); it is null for query-parameter errors.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Violation(Integer index, String field, String message) {

    public static Violation ofEvent(int index, String field, String message) {
        return new Violation(index, field, message);
    }

    public static Violation ofParam(String field, String message) {
        return new Violation(null, field, message);
    }
}
