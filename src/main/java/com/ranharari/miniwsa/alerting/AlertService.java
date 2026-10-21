package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;

public interface AlertService {

    /**
     * Gives the rule an id and stores it. It's part of the next evaluation within moments.
     *
     * @throws ServiceUnavailableException if the rule could not be stored
     */
    AlertRule define(AlertRuleRequest request);

    /**
     * Checks every rule against the current counts.
     *
     * @throws ServiceUnavailableException while the alerting app is starting or rebalancing
     */
    AlertEvaluation evaluate();
}
