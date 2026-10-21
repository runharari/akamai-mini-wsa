package com.ranharari.miniwsa.common.web;

import com.ranharari.miniwsa.common.exception.Violation;

import java.util.List;

/** Body of every 4xx/5xx response: {"errors":[{index, field, message}]}. */
public record ApiErrorResponse(List<Violation> errors) {

    public static ApiErrorResponse of(String message) {
        return new ApiErrorResponse(List.of(new Violation(null, null, message)));
    }
}
