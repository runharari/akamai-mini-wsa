package com.ranharari.miniwsa.query.config;

import com.ranharari.miniwsa.query.ClickHouseSamplesRepository;
import com.ranharari.miniwsa.query.ClickHouseStatsRepository;
import com.ranharari.miniwsa.query.DefaultSamplesService;
import com.ranharari.miniwsa.query.DefaultStatsService;
import com.ranharari.miniwsa.query.DefaultTimeRangeResolver;
import com.ranharari.miniwsa.query.SamplesController;
import com.ranharari.miniwsa.query.SamplesRepository;
import com.ranharari.miniwsa.query.SamplesService;
import com.ranharari.miniwsa.query.StatsController;
import com.ranharari.miniwsa.query.StatsRepository;
import com.ranharari.miniwsa.query.StatsService;
import com.ranharari.miniwsa.query.TimeRangeResolver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.Clock;

/** The query role: stats summary and samples, read from ClickHouse. */
@Configuration
@Profile({"query", "default"})
public class QueryConfig {

    @Bean
    TimeRangeResolver timeRangeResolver(Clock clock) {
        return new DefaultTimeRangeResolver(clock);
    }

    @Bean
    StatsRepository statsRepository(JdbcClient jdbcClient) {
        return new ClickHouseStatsRepository(jdbcClient);
    }

    @Bean
    StatsService statsService(StatsRepository repository) {
        return new DefaultStatsService(repository);
    }

    @Bean
    SamplesRepository samplesRepository(JdbcClient jdbcClient) {
        return new ClickHouseSamplesRepository(jdbcClient);
    }

    @Bean
    SamplesService samplesService(SamplesRepository repository) {
        return new DefaultSamplesService(repository);
    }

    @Bean
    StatsController statsController(TimeRangeResolver timeRangeResolver, StatsService statsService) {
        return new StatsController(timeRangeResolver, statsService);
    }

    @Bean
    SamplesController samplesController(TimeRangeResolver timeRangeResolver, SamplesService samplesService) {
        return new SamplesController(timeRangeResolver, samplesService);
    }
}
