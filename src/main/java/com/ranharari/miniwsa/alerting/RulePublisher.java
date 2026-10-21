package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;

/** Stores a rule durably. */
public interface RulePublisher {

    /** @throws ServiceUnavailableException if the rule could not be stored */
    void publish(AlertRule rule);
}
