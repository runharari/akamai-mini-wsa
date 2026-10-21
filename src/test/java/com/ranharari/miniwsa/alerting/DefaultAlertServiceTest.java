package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.alerting.AlertEvaluation.FiringAlert;
import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;
import com.ranharari.miniwsa.common.metrics.MicrometerMetricsFactory;
import com.ranharari.miniwsa.common.model.Category;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultAlertServiceTest {

    private static final Instant NOW = Instant.parse("2026-05-20T14:10:30Z");

    @Mock
    private RulePublisher publisher;
    @Mock
    private AlertStateReader state;

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

    private DefaultAlertService service() {
        return new DefaultAlertService(publisher, state, new MicrometerMetricsFactory(meters),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void defineGivesTheRuleAnIdAndPublishesIt() {
        AlertRule rule = service().define(new AlertRuleRequest("SQLi burst", Category.INJECTION, 14227L, 100L, 5));

        ArgumentCaptor<AlertRule> published = ArgumentCaptor.forClass(AlertRule.class);
        verify(publisher).publish(published.capture());
        assertThat(published.getValue()).isEqualTo(rule);
        assertThat(rule.id()).isNotBlank();
        assertThat(rule).extracting(AlertRule::category, AlertRule::configId, AlertRule::threshold, AlertRule::windowMinutes)
                .containsExactly(Category.INJECTION, 14227L, 100L, 5);
        assertThat(meters.get("wsa.alerting.rules.defined").counter().count()).isEqualTo(1);
    }

    @Test
    void defineFailsWhenTheRuleCannotBeStoredAndCountsNothing() {
        doThrow(new ServiceUnavailableException("down", null)).when(publisher).publish(any());

        assertThatThrownBy(() -> service().define(new AlertRuleRequest(null, Category.BOT, null, 1L, 1)))
                .isInstanceOf(ServiceUnavailableException.class);
        assertThat(meters.get("wsa.alerting.rules.defined").counter().count()).isZero();
    }

    @Test
    void firesOnlyWhenTheCountIsAboveTheThreshold() {
        AlertRule above = new AlertRule("above", "above", Category.INJECTION, 14227L, 5, 10);
        AlertRule equal = new AlertRule("equal", "equal", Category.BOT, 14227L, 5, 10);
        when(state.rules()).thenReturn(List.of(above, equal));
        when(state.count(any(), any(), any())).thenReturn(6L, 5L);

        AlertEvaluation evaluation = service().evaluate();

        assertThat(evaluation.evaluatedAt()).isEqualTo(NOW);
        assertThat(evaluation.rulesChecked()).isEqualTo(2);
        assertThat(evaluation.firing()).containsExactly(FiringAlert.of(above, 6));
        assertThat(meters.get("wsa.alerting.firing").summary().totalAmount()).isEqualTo(1);
    }

    @Test
    void countsTheRulesConfigOrAllConfigsFromTheStartOfTheWindowsFirstMinute() {
        AlertRule oneConfig = new AlertRule("one", null, Category.INJECTION, 14227L, 5, 10);
        AlertRule allConfigs = new AlertRule("all", null, Category.DOS, null, 5, 60);
        when(state.rules()).thenReturn(List.of(oneConfig, allConfigs));

        service().evaluate();

        // 14:10:30 minus 10 minutes = 14:00:30, rounded down to the minute it starts in.
        verify(state).count("14227|INJECTION", Instant.parse("2026-05-20T14:00:00Z"), NOW);
        verify(state).count("*|DOS", Instant.parse("2026-05-20T13:10:00Z"), NOW);
    }

    @Test
    void evaluateWithNoRulesFiresNothing() {
        when(state.rules()).thenReturn(List.of());

        AlertEvaluation evaluation = service().evaluate();

        assertThat(evaluation.rulesChecked()).isZero();
        assertThat(evaluation.firing()).isEmpty();
        verifyNoInteractions(publisher);
    }
}
