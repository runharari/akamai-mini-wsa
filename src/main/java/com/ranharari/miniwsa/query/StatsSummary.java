package com.ranharari.miniwsa.query;

import java.util.List;
import java.util.Map;

/** The answer of {@code GET /v1/stats/summary}. {@code configId} is null when all configs are counted. */
public record StatsSummary(
        Long configId,
        TimeRange timeRange,
        long totalEvents,
        Map<String, CategoryStats> byCategory,
        Map<String, Long> byAction,
        List<AttackerStats> topAttackers,
        List<PathStats> topTargetedPaths) {

    public record CategoryStats(long count, double avgThreatScore) {
    }

    public record AttackerStats(String clientIp, long count, double avgThreatScore) {
    }

    public record PathStats(String path, long count) {
    }
}
