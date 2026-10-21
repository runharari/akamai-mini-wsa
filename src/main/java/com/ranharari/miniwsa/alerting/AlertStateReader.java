package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;

import java.time.Instant;
import java.util.List;

/**
 * Reads the alerting app's current state: the rules and the event counts.
 * Both methods throw {@link ServiceUnavailableException} while the app is starting or rebalancing.
 */
public interface AlertStateReader {

    List<AlertRule> rules();

    /** Events counted under {@code countKey} in the minutes starting from {@code from} up to {@code to}. */
    long count(String countKey, Instant from, Instant to);
}
