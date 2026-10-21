package com.ranharari.miniwsa.common.error;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * One problem in a request. {@code index} is the position of the event in an ingest batch
 * (0 for a single object); it is null for query-parameter errors.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FieldError(Integer index, String field, String message) {

    public static FieldError ofEvent(int index, String field, String message) {
        return new FieldError(index, field, message);
    }

    public static FieldError ofParam(String field, String message) {
        return new FieldError(null, field, message);
    }
}
