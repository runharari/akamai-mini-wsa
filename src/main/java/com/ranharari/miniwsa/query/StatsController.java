package com.ranharari.miniwsa.query;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@Profile({"query", "default"})
public class StatsController {

    private final TimeRangeResolver timeRangeResolver;
    private final StatsService statsService;

    public StatsController(TimeRangeResolver timeRangeResolver, StatsService statsService) {
        this.timeRangeResolver = timeRangeResolver;
        this.statsService = statsService;
    }

    /** Summary of the events in [from, to). Without from/to: the last 24 hours. Without configId: all configs. */
    @GetMapping("/v1/stats/summary")
    public StatsSummary summary(@RequestParam(required = false) Long configId,
                                @RequestParam(required = false) Instant from,
                                @RequestParam(required = false) Instant to) {
        TimeRange range = timeRangeResolver.resolve(from, to);
        return statsService.summary(configId, range);
    }
}
