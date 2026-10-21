package com.ranharari.miniwsa.common.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** The WAF rule that matched the request. */
public record Rule(
        @NotBlank String id,
        String name,
        String message,
        @NotNull Severity severity,
        @NotNull Category category) {
}
