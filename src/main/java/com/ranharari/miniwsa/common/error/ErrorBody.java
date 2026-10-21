package com.ranharari.miniwsa.common.error;

import java.util.List;

/** Body of every 4xx/5xx response: {"errors":[{index, field, message}]}. */
public record ErrorBody(List<FieldError> errors) {

    public static ErrorBody of(String message) {
        return new ErrorBody(List.of(new FieldError(null, null, message)));
    }
}
