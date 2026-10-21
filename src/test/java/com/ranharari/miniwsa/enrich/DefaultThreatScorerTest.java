package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.Rule;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.common.model.Severity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultThreatScorerTest {

    private static final String NEUTRAL_PATH = "/api/items";

    private final DefaultThreatScorer scorer = new DefaultThreatScorer();

    @ParameterizedTest
    @CsvSource({"CRITICAL, 40", "HIGH, 30", "MEDIUM, 20", "LOW, 10"})
    void severityPoints(Severity severity, int expected) {
        assertThat(scorer.score(event(severity, Action.MONITOR, NEUTRAL_PATH), false)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"DENY, 30", "ALERT, 20", "MONITOR, 10"})
    void actionPointsOnTopOfLowSeverity(Action action, int expected) {
        assertThat(scorer.score(event(Severity.LOW, action, NEUTRAL_PATH), false)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"/admin", "/api/v1/login", "/Admin/users", "/LOGIN", "/administrator"})
    void sensitivePathAddsBonus(String path) {
        assertThat(scorer.score(event(Severity.LOW, Action.MONITOR, path), false)).isEqualTo(25);
    }

    @ParameterizedTest
    @CsvSource({"/logout", "/api/items", "/"})
    void otherPathsAddNothing(String path) {
        assertThat(scorer.score(event(Severity.LOW, Action.MONITOR, path), false)).isEqualTo(10);
    }

    @Test
    void repeatOffenderAddsBonus() {
        assertThat(scorer.score(event(Severity.LOW, Action.MONITOR, NEUTRAL_PATH), true)).isEqualTo(25);
    }

    @Test
    void neverExceeds100_maxReachableIs90() {
        assertThat(scorer.score(event(Severity.CRITICAL, Action.DENY, "/admin"), true)).isEqualTo(90);
    }

    private static SecurityEvent event(Severity severity, Action action, String path) {
        return new SecurityEvent("evt-1", Instant.parse("2026-05-20T14:32:10Z"), 14227L, "pol_web1",
                "203.0.113.42", "www.example.com", path, "GET", 403, "Mozilla/5.0",
                new Rule("950001", "RULE", "message", severity, Category.INJECTION),
                action, null, null, null, null);
    }
}
