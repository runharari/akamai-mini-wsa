package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.query.StatsSummary.AttackerStats;
import com.ranharari.miniwsa.query.StatsSummary.CategoryStats;
import com.ranharari.miniwsa.query.StatsSummary.PathStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultStatsServiceTest {

    private static final TimeRange RANGE =
            new TimeRange(Instant.parse("2026-05-20T00:00:00Z"), Instant.parse("2026-05-21T00:00:00Z"));

    @Mock
    private StatsRepository repository;

    @InjectMocks
    private DefaultStatsService service;

    @Test
    void assemblesTheSummaryFromEachRepositoryBlock() {
        Map<String, CategoryStats> byCategory = Map.of("BOT", new CategoryStats(2, 45.5));
        Map<String, Long> byAction = Map.of("DENY", 2L);
        List<AttackerStats> attackers = List.of(new AttackerStats("1.2.3.4", 2, 45.5));
        List<PathStats> paths = List.of(new PathStats("/login", 2));
        when(repository.totalEvents(14227L, RANGE)).thenReturn(2L);
        when(repository.byCategory(14227L, RANGE)).thenReturn(byCategory);
        when(repository.byAction(14227L, RANGE)).thenReturn(byAction);
        when(repository.topAttackers(14227L, RANGE)).thenReturn(attackers);
        when(repository.topTargetedPaths(14227L, RANGE)).thenReturn(paths);

        StatsSummary summary = service.summary(14227L, RANGE);

        assertThat(summary).isEqualTo(new StatsSummary(14227L, RANGE, 2, byCategory, byAction, attackers, paths));
        verify(repository).totalEvents(14227L, RANGE);
        verify(repository).byCategory(14227L, RANGE);
        verify(repository).byAction(14227L, RANGE);
        verify(repository).topAttackers(14227L, RANGE);
        verify(repository).topTargetedPaths(14227L, RANGE);
        verifyNoMoreInteractions(repository);
    }

    @Test
    void passesANullConfigIdThrough() {
        when(repository.byCategory(null, RANGE)).thenReturn(Map.of());
        when(repository.byAction(null, RANGE)).thenReturn(Map.of());
        when(repository.topAttackers(null, RANGE)).thenReturn(List.of());
        when(repository.topTargetedPaths(null, RANGE)).thenReturn(List.of());

        StatsSummary summary = service.summary(null, RANGE);

        assertThat(summary.configId()).isNull();
        assertThat(summary.totalEvents()).isZero();
        verify(repository).totalEvents(null, RANGE);
    }
}
