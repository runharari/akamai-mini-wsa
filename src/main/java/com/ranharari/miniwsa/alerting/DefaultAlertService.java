package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.alerting.AlertEvaluation.FiringAlert;
import com.ranharari.miniwsa.common.metrics.MetricsFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DefaultAlertService implements AlertService {

    private static final Logger log = LoggerFactory.getLogger(DefaultAlertService.class);

    private final RulePublisher publisher;
    private final AlertStateReader state;
    private final Clock clock;

    private final Counter rulesDefined;
    private final DistributionSummary firingPerEvaluation;

    public DefaultAlertService(RulePublisher publisher, AlertStateReader state, MetricsFactory metrics, Clock clock) {
        this.publisher = publisher;
        this.state = state;
        this.clock = clock;
        this.rulesDefined = metrics.counter("alerting.rules.defined", "Alert rules defined");
        this.firingPerEvaluation = metrics.summary("alerting.firing", "Alerts firing per evaluation");
    }

    @Override
    public AlertRule define(AlertRuleRequest request) {
        AlertRule rule = new AlertRule(UUID.randomUUID().toString(), request.name(), request.category(),
                request.configId(), request.threshold(), request.windowMinutes());
        publisher.publish(rule);
        rulesDefined.increment();
        log.info("Alert rule defined id={} category={} configId={} threshold={} windowMinutes={}",
                rule.id(), rule.category(), rule.configId(), rule.threshold(), rule.windowMinutes());
        return rule;
    }

    /**
     * A rule fires when more than {@code threshold} events happened in its window, which ends now.
     * Counts are per whole minute, so the window starts at the beginning of its first minute.
     */
    @Override
    public AlertEvaluation evaluate() {
        Instant now = clock.instant().truncatedTo(ChronoUnit.SECONDS); // whole seconds read better
        List<AlertRule> rules = state.rules();
        List<FiringAlert> firing = new ArrayList<>();
        for (AlertRule rule : rules) {
            Instant from = now.minus(Duration.ofMinutes(rule.windowMinutes())).truncatedTo(ChronoUnit.MINUTES);
            long count = state.count(CountKey.of(rule.configId(), rule.category()), from, now);
            if (count > rule.threshold()) {
                firing.add(FiringAlert.of(rule, count));
            }
        }
        firingPerEvaluation.record(firing.size());
        return new AlertEvaluation(now, rules.size(), firing);
    }
}
