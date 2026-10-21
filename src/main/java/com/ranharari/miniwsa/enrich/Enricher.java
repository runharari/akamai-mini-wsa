package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.SecurityEvent;

/** Adds attackType and threatScore to an event. */
public interface Enricher {

    EnrichedEvent enrich(SecurityEvent event, boolean repeatOffender);
}
