package com.ranharari.miniwsa.query;


public class DefaultStatsService implements StatsService {

    private final StatsRepository repository;

    public DefaultStatsService(StatsRepository repository) {
        this.repository = repository;
    }

    @Override
    public StatsSummary summary(Long configId, TimeRange range) {
        return new StatsSummary(
                configId,
                range,
                repository.totalEvents(configId, range),
                repository.byCategory(configId, range),
                repository.byAction(configId, range),
                repository.topAttackers(configId, range),
                repository.topTargetedPaths(configId, range));
    }
}
