package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** The compacted alert.rules topic is the rules' database: Kafka keeps the latest version of every rule. */
public class KafkaRulePublisher implements RulePublisher {

    private static final long TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, AlertRule> kafkaTemplate;

    public KafkaRulePublisher(KafkaTemplate<String, AlertRule> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(AlertRule rule) {
        try {
            kafkaTemplate.send(Topics.ALERT_RULES, rule.id(), rule).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServiceUnavailableException("Interrupted while saving the rule", e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw new ServiceUnavailableException("Could not save the rule, retry later", e);
        }
    }
}
