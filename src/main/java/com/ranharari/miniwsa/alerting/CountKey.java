package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.model.Category;

/**
 * The key events are counted under. Every event is counted twice: once for its config
 * ("14227|INJECTION") and once for all configs ("*|INJECTION"), so any rule is a single lookup.
 */
final class CountKey {

    /** Counts are kept for a day, so no rule may look further back. */
    static final int MAX_WINDOW_MINUTES = 24 * 60;

    private static final String ALL_CONFIGS = "*";

    private CountKey() {
    }

    static String of(Long configId, Category category) {
        return (configId != null ? configId.toString() : ALL_CONFIGS) + "|" + category.name();
    }

    static String allConfigs(Category category) {
        return of(null, category);
    }
}
