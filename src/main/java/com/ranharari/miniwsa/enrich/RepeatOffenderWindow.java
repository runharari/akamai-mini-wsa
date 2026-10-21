package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.SecurityEvent;

import java.time.Instant;
import java.util.List;

/**
 * Decides whether an event's IP is a repeat offender, given what we remember about that IP.
 * Pure: the caller loads the IP's history, calls {@link #check}, and saves the returned history.
 */
public interface RepeatOffenderWindow {

    Result check(History history, SecurityEvent event);

    /** What we remember about one IP: its most recent events (bounded), newest first. */
    record History(List<Entry> entries) {

        public static History empty() {
            return new History(List.of());
        }
    }

    record Entry(String eventId, Instant timestamp) {
    }

    /** The decision for this event and the history to save for the IP. */
    record Result(boolean repeatOffender, History updated) {
    }
}
