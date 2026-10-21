package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.SecurityEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * "More than 5 events from the same IP within the last 10 minutes."
 * <p>
 * Uses the events' own timestamps, not the clock: loading 6 hours of generated events in a few
 * seconds must not make every IP a repeat offender.
 * <p>
 * Only the newest 6 entries per IP are kept. That is enough to answer "more than 5?" and keeps
 * memory tiny even when one IP floods us. Trade-off: an event arriving very late (older than
 * the 6 kept entries) can be under-counted.
 */
public class DefaultRepeatOffenderWindow implements RepeatOffenderWindow {

    static final Duration WINDOW = Duration.ofMinutes(10);
    static final int THRESHOLD = 5;
    static final int MAX_ENTRIES = THRESHOLD + 1;

    @Override
    public Result check(History history, SecurityEvent event) {
        Instant time = event.timestamp();
        Instant windowStart = time.minus(WINDOW);

        // The event itself never counts; a re-delivered event (same id) isn't counted twice either.
        long earlierEvents = history.entries().stream()
                .filter(entry -> !entry.eventId().equals(event.eventId()))
                .filter(entry -> !entry.timestamp().isBefore(windowStart) && !entry.timestamp().isAfter(time))
                .count();
        boolean repeatOffender = earlierEvents > THRESHOLD;

        return new Result(repeatOffender, remember(history, event));
    }

    /** Adds the event (once) and keeps only the newest entries. */
    private static History remember(History history, SecurityEvent event) {
        boolean alreadyKnown = history.entries().stream().anyMatch(entry -> entry.eventId().equals(event.eventId()));
        if (alreadyKnown) {
            return history;
        }
        List<Entry> entries = new ArrayList<>(history.entries());
        entries.add(new Entry(event.eventId(), event.timestamp()));
        entries.sort(Comparator.comparing(Entry::timestamp).reversed());
        return new History(List.copyOf(entries.subList(0, Math.min(MAX_ENTRIES, entries.size()))));
    }
}
