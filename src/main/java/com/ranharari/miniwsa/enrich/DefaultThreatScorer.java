package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.common.model.Severity;

import java.util.List;
import java.util.Locale;

public class DefaultThreatScorer implements ThreatScorer {

    private static final int CRITICAL_POINTS = 40;
    private static final int HIGH_POINTS = 30;
    private static final int MEDIUM_POINTS = 20;
    private static final int LOW_POINTS = 10;

    private static final int DENY_POINTS = 20;
    private static final int ALERT_POINTS = 10;
    private static final int MONITOR_POINTS = 0;

    private static final int PATH_BONUS = 15;
    private static final int REPEAT_OFFENDER_BONUS = 15;
    private static final int MAX_SCORE = 100;
    private static final List<String> SENSITIVE_PATHS = List.of("/admin", "/login");

    @Override
    public int score(SecurityEvent event, boolean repeatOffender) {
        int total = severityPoints(event.rule().severity())
                + actionPoints(event.action())
                + (isSensitivePath(event.path()) ? PATH_BONUS : 0)
                + (repeatOffender ? REPEAT_OFFENDER_BONUS : 0);
        // With today's rules the max is 90; the cap guards future rules.
        return Math.min(MAX_SCORE, total);
    }

    private int severityPoints(Severity severity) {
        return switch (severity) {
            case CRITICAL -> CRITICAL_POINTS;
            case HIGH -> HIGH_POINTS;
            case MEDIUM -> MEDIUM_POINTS;
            case LOW -> LOW_POINTS;
        };
    }

    private int actionPoints(Action action) {
        return switch (action) {
            case DENY -> DENY_POINTS;
            case ALERT -> ALERT_POINTS;
            case MONITOR -> MONITOR_POINTS;
        };
    }

    private boolean isSensitivePath(String path) {
        String lower = path.toLowerCase(Locale.ROOT);
        return SENSITIVE_PATHS.stream().anyMatch(lower::contains);
    }
}
