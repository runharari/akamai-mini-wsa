package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.model.Category;

import java.time.Instant;
import java.util.List;

/** The answer of {@code GET /v1/alerts/evaluate}: how many rules were checked and which are firing. */
public record AlertEvaluation(Instant evaluatedAt, int rulesChecked, List<FiringAlert> firing) {

    public record FiringAlert(String ruleId, String name, Category category, Long configId, long threshold,
                              int windowMinutes, long count) {

        static FiringAlert of(AlertRule rule, long count) {
            return new FiringAlert(rule.id(), rule.name(), rule.category(), rule.configId(), rule.threshold(),
                    rule.windowMinutes(), count);
        }
    }
}
