package com.ranharari.miniwsa.common.error;

import java.util.List;

/** The client sent something we can't accept. Mapped to 400 with the listed errors. */
public class InvalidRequestException extends RuntimeException {

    private final List<FieldError> errors;

    public InvalidRequestException(List<FieldError> errors) {
        super("Invalid request: " + errors.size() + " error(s)");
        this.errors = List.copyOf(errors);
    }

    public InvalidRequestException(FieldError error) {
        this(List.of(error));
    }

    public List<FieldError> errors() {
        return errors;
    }
}
