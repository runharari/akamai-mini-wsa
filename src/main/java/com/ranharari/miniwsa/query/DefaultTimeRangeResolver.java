package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.exception.Violation;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Component
@Profile({"query", "default"})
public class DefaultTimeRangeResolver implements TimeRangeResolver {

    private static final Duration DEFAULT_WINDOW = Duration.ofHours(24);

    private final Clock clock;

    public DefaultTimeRangeResolver(Clock clock) {
        this.clock = clock;
    }

    @Override
    public TimeRange resolve(Instant from, Instant to) {
        Instant end = to != null ? to : clock.instant().truncatedTo(ChronoUnit.SECONDS); // whole seconds read better
        Instant start = from != null ? from : end.minus(DEFAULT_WINDOW);
        if (!start.isBefore(end)) {
            throw new InvalidRequestException(Violation.ofParam("from", "must be before to"));
        }
        return new TimeRange(start, end);
    }
}
