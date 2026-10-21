package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.model.Category;

/**
 * "More than {@code threshold} events of {@code category} within the last {@code windowMinutes}."
 * A null {@code configId} means across all configs; {@code name} is optional.
 */
public record AlertRule(String id, String name, Category category, Long configId, long threshold, int windowMinutes) {
}
