package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.SecurityEvent;

/** Computes the 0-100 threat score of an event. */
public interface ThreatScorer {

    /**
     * @param repeatOffender whether the event's IP sent more than 5 events in the 10 minutes before it
     *                       (decided by {@link RepeatOffenderWindow}, which needs history the scorer doesn't have)
     */
    int score(SecurityEvent event, boolean repeatOffender);
}
