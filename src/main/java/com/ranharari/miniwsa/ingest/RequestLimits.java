package com.ranharari.miniwsa.ingest;

import java.time.Duration;

/**
 * What one ingest request may contain.
 *
 * @param maxBatchSize most events in one request
 * @param maxEventAge  oldest event timestamp accepted
 * @param maxClockSkew how far in the future an event timestamp may be
 */
public record RequestLimits(int maxBatchSize, Duration maxEventAge, Duration maxClockSkew) {
}
