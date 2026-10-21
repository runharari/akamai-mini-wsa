package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.exception.StorageException;
import com.ranharari.miniwsa.query.StatsSummary.AttackerStats;
import com.ranharari.miniwsa.query.StatsSummary.CategoryStats;
import com.ranharari.miniwsa.query.StatsSummary.PathStats;

import java.util.List;
import java.util.Map;

/**
 * One method per block of the summary. A null {@code configId} means all configs.
 * All methods throw {@link StorageException} if the query fails.
 */
public interface StatsRepository {

    long totalEvents(Long configId, TimeRange range);

    /** Events and rounded average threat score per category, biggest first. */
    Map<String, CategoryStats> byCategory(Long configId, TimeRange range);

    /** Events per action, biggest first. */
    Map<String, Long> byAction(Long configId, TimeRange range);

    /** The 10 client IPs with the most events, biggest first. */
    List<AttackerStats> topAttackers(Long configId, TimeRange range);

    /** The 10 paths with the most events, biggest first. */
    List<PathStats> topTargetedPaths(Long configId, TimeRange range);
}
