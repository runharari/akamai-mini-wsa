package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow.Entry;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow.History;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow.Result;
import com.ranharari.miniwsa.support.TestEvents;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultRepeatOffenderWindowTest {

    private static final Instant T0 = Instant.parse("2026-05-20T14:00:00Z");
    private static final String IP = "203.0.113.42";

    private final DefaultRepeatOffenderWindow window = new DefaultRepeatOffenderWindow();

    @Test
    void firstEventIsNotRepeatOffender() {
        Result result = window.check(History.empty(), event("e1", 0));

        assertThat(result.repeatOffender()).isFalse();
        assertThat(result.updated().entries()).containsExactly(new Entry("e1", T0));
    }

    @Test
    void fiveEarlierEventsAreNotEnoughButSixAre() {
        History history = History.empty();
        for (int i = 1; i <= 5; i++) {
            history = window.check(history, event("e" + i, i)).updated();
        }

        Result sixth = window.check(history, event("e6", 6));
        Result seventh = window.check(sixth.updated(), event("e7", 7));

        assertThat(sixth.repeatOffender()).as("5 earlier events").isFalse();
        assertThat(seventh.repeatOffender()).as("6 earlier events").isTrue();
    }

    @Test
    void eventsExactlyTenMinutesOldStillCountButOlderOnesDont() {
        History sixAtT0 = historyOf(6, 0);

        assertThat(window.check(sixAtT0, event("x", 10 * 60)).repeatOffender()).as("exactly 10 min").isTrue();
        assertThat(window.check(sixAtT0, event("y", 10 * 60 + 1)).repeatOffender()).as("10 min + 1 s").isFalse();
    }

    @Test
    void laterEventsInTheHistoryDontCountForAnEarlierEvent() {
        History sixLater = historyOf(6, 300);

        assertThat(window.check(sixLater, event("late-arrival", 0)).repeatOffender()).isFalse();
    }

    @Test
    void outOfOrderArrivalStillCountsEarlierEvents() {
        History history = History.empty();
        for (int second : new int[] {50, 10, 40, 20, 30, 60}) {
            history = window.check(history, event("e" + second, second)).updated();
        }

        assertThat(window.check(history, event("e70", 70)).repeatOffender()).isTrue();
    }

    @Test
    void replayedEventIsNotCountedTwiceNorStoredTwice() {
        History fiveEvents = historyOf(5, 0);
        SecurityEvent sixth = event("e-dup", 10);
        History withSixth = window.check(fiveEvents, sixth).updated();

        Result replay = window.check(withSixth, sixth);

        assertThat(replay.repeatOffender()).as("still only 5 other events").isFalse();
        assertThat(replay.updated().entries()).hasSize(6);
    }

    @Test
    void keepsOnlyTheNewestSixEntries() {
        History history = historyOf(10, 0);

        assertThat(history.entries()).hasSize(DefaultRepeatOffenderWindow.MAX_ENTRIES);
        assertThat(history.entries()).extracting(Entry::eventId)
                .containsExactly("h9", "h8", "h7", "h6", "h5", "h4");
    }

    /** {@code count} events from the IP, one second apart, starting {@code startSecond} after T0. */
    private History historyOf(int count, int startSecond) {
        History history = History.empty();
        for (int i = 0; i < count; i++) {
            history = window.check(history, event("h" + i, startSecond + i)).updated();
        }
        return history;
    }

    private static SecurityEvent event(String eventId, int secondsAfterT0) {
        return TestEvents.event(eventId, IP, T0.plusSeconds(secondsAfterT0));
    }
}
