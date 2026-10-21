package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultTimeRangeResolverTest {

    private static final Instant NOW = Instant.parse("2026-05-21T10:00:00Z");

    private final DefaultTimeRangeResolver resolver =
            new DefaultTimeRangeResolver(Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void defaultsToTheLast24HoursWhenNothingIsGiven() {
        assertThat(resolver.resolve(null, null))
                .isEqualTo(new TimeRange(Instant.parse("2026-05-20T10:00:00Z"), NOW));
    }

    @Test
    void usesNowAsEndWhenOnlyFromIsGiven() {
        Instant from = Instant.parse("2026-05-21T08:00:00Z");

        assertThat(resolver.resolve(from, null)).isEqualTo(new TimeRange(from, NOW));
    }

    @Test
    void startsOneDayBeforeToWhenOnlyToIsGiven() {
        Instant to = Instant.parse("2026-05-10T12:00:00Z");

        assertThat(resolver.resolve(null, to))
                .isEqualTo(new TimeRange(Instant.parse("2026-05-09T12:00:00Z"), to));
    }

    @Test
    void keepsBothBoundsWhenGiven() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to = Instant.parse("2026-05-02T00:00:00Z");

        assertThat(resolver.resolve(from, to)).isEqualTo(new TimeRange(from, to));
    }

    @Test
    void rejectsFromEqualToTo() {
        Instant same = Instant.parse("2026-05-01T00:00:00Z");

        assertThatThrownBy(() -> resolver.resolve(same, same))
                .isInstanceOfSatisfying(InvalidRequestException.class, e -> {
                    assertThat(e.violations()).hasSize(1);
                    assertThat(e.violations().getFirst().field()).isEqualTo("from");
                    assertThat(e.violations().getFirst().message()).isEqualTo("must be before to");
                });
    }

    @Test
    void rejectsFromAfterTo() {
        Instant from = Instant.parse("2026-05-02T00:00:00Z");
        Instant to = Instant.parse("2026-05-01T00:00:00Z");

        assertThatThrownBy(() -> resolver.resolve(from, to)).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    void rejectsFromInTheFutureWhenToIsMissing() {
        Instant from = Instant.parse("2026-06-01T00:00:00Z");

        assertThatThrownBy(() -> resolver.resolve(from, null)).isInstanceOf(InvalidRequestException.class);
    }
}
