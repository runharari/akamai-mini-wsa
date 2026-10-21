package com.ranharari.miniwsa.common.exception;

import java.util.List;

/** The client sent something we can't accept. Mapped to 400 listing every violation. */
public class InvalidRequestException extends RuntimeException {

    private final List<Violation> violations;

    public InvalidRequestException(List<Violation> violations) {
        super("Invalid request: " + violations.size() + " violation(s)");
        this.violations = List.copyOf(violations);
    }

    public InvalidRequestException(Violation violation) {
        this(List.of(violation));
    }

    public List<Violation> violations() {
        return violations;
    }
}
