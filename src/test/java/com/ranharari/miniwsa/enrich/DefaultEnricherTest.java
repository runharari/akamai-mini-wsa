package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.support.TestEvents;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultEnricherTest {

    private static final SecurityEvent EVENT =
            TestEvents.event("evt-1", "203.0.113.42", Instant.parse("2026-05-20T14:32:10Z"));

    @Mock
    private AttackClassifier classifier;
    @Mock
    private ThreatScorer scorer;

    @InjectMocks
    private DefaultEnricher enricher;

    @Test
    void addsAttackTypeAndScore() {
        when(classifier.attackType(Category.INJECTION)).thenReturn("SQL/Command Injection");
        when(scorer.score(EVENT, true)).thenReturn(90);

        EnrichedEvent result = enricher.enrich(EVENT, true);

        assertThat(result.event()).isSameAs(EVENT);
        assertThat(result.attackType()).isEqualTo("SQL/Command Injection");
        assertThat(result.threatScore()).isEqualTo(90);
        verify(classifier).attackType(Category.INJECTION);
        verify(scorer).score(EVENT, true);
        verifyNoMoreInteractions(classifier, scorer);
    }

    @Test
    void passesRepeatOffenderFalseToScorer() {
        when(classifier.attackType(Category.INJECTION)).thenReturn("SQL/Command Injection");
        when(scorer.score(EVENT, false)).thenReturn(75);

        EnrichedEvent result = enricher.enrich(EVENT, false);

        assertThat(result.threatScore()).isEqualTo(75);
        verify(classifier).attackType(Category.INJECTION);
        verify(scorer).score(EVENT, false);
        verifyNoMoreInteractions(classifier, scorer);
    }
}
