package com.ranharari.miniwsa.query;

import java.time.Instant;

/** A half-open time window: {@code from} is inclusive, {@code to} is exclusive. */
public record TimeRange(Instant from, Instant to) {
}
