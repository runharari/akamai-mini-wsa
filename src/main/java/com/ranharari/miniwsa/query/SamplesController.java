package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.exception.Violation;
import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@Profile({"query", "default"})
public class SamplesController {

    private static final int MAX_LIMIT = 100;

    private final TimeRangeResolver timeRangeResolver;
    private final SamplesService samplesService;

    public SamplesController(TimeRangeResolver timeRangeResolver, SamplesService samplesService) {
        this.timeRangeResolver = timeRangeResolver;
        this.samplesService = samplesService;
    }

    /** A page of events in [from, to), newest first, optionally filtered. Without from/to: the last 24 hours. */
    @GetMapping("/v1/events/samples")
    public SamplesPage samples(@RequestParam(required = false) Long configId,
                               @RequestParam(required = false) Instant from,
                               @RequestParam(required = false) Instant to,
                               @RequestParam(required = false) Category category,
                               @RequestParam(required = false) Action action,
                               @RequestParam(defaultValue = "20") int limit,
                               @RequestParam(defaultValue = "0") int offset) {
        checkLimit(limit);
        checkOffset(offset);
        TimeRange range = timeRangeResolver.resolve(from, to);
        return samplesService.samples(new SampleFilter(configId, range, category, action), limit, offset);
    }

    private static void checkLimit(int limit) {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new InvalidRequestException(Violation.ofParam("limit", "must be between 1 and " + MAX_LIMIT));
        }
    }

    private static void checkOffset(int offset) {
        if (offset < 0) {
            throw new InvalidRequestException(Violation.ofParam("offset", "must not be negative"));
        }
    }
}
