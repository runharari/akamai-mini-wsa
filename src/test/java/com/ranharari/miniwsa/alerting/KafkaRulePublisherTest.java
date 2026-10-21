package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.config.Topics;
import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;
import com.ranharari.miniwsa.common.model.Category;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaRulePublisherTest {

    private static final AlertRule RULE = new AlertRule("r1", null, Category.BOT, null, 10, 5);

    @Mock
    private KafkaTemplate<String, AlertRule> kafkaTemplate;

    @Test
    void sendsTheRuleKeyedByItsId() {
        when(kafkaTemplate.send(Topics.ALERT_RULES, "r1", RULE)).thenReturn(CompletableFuture.completedFuture(null));

        new KafkaRulePublisher(kafkaTemplate).publish(RULE);

        verify(kafkaTemplate).send(Topics.ALERT_RULES, "r1", RULE);
    }

    @Test
    void failsWhenKafkaDoesNotStoreIt() {
        when(kafkaTemplate.send(Topics.ALERT_RULES, "r1", RULE))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

        assertThatThrownBy(() -> new KafkaRulePublisher(kafkaTemplate).publish(RULE))
                .isInstanceOf(ServiceUnavailableException.class);
    }
}
