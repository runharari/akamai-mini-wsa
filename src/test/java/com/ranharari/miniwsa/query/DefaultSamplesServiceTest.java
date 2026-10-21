package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.Rule;
import com.ranharari.miniwsa.common.model.Severity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultSamplesServiceTest {

    private static final TimeRange RANGE =
            new TimeRange(Instant.parse("2026-05-20T00:00:00Z"), Instant.parse("2026-05-21T00:00:00Z"));
    private static final SampleFilter FILTER = new SampleFilter(14227L, RANGE, Category.INJECTION, Action.DENY);

    @Mock
    private SamplesRepository repository;

    @InjectMocks
    private DefaultSamplesService service;

    @Test
    void returnsTheTotalAndThePageFromTheRepository() {
        EventSample sample = new EventSample("evt-1", Instant.parse("2026-05-20T14:32:10Z"),
                Instant.parse("2026-05-20T14:32:11Z"), 14227L, null, "203.0.113.42", null, "/login", "POST", 403, null,
                new Rule("950001", null, null, Severity.CRITICAL, Category.INJECTION), Action.DENY, null, null, null,
                "SQL/Command Injection", 75);
        when(repository.count(FILTER)).thenReturn(57L);
        when(repository.find(FILTER, 20, 40)).thenReturn(List.of(sample));

        SamplesPage page = service.samples(FILTER, 20, 40);

        assertThat(page).isEqualTo(new SamplesPage(57, 20, 40, List.of(sample)));
        verify(repository).count(FILTER);
        verify(repository).find(FILTER, 20, 40);
        verifyNoMoreInteractions(repository);
    }
}
