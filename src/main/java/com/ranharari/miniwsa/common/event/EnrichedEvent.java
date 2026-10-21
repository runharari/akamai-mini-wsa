package com.ranharari.miniwsa.common.event;

/** A security event after enrichment: the original event plus the computed fields. */
public record EnrichedEvent(SecurityEvent event, String attackType, int threatScore) {
}
