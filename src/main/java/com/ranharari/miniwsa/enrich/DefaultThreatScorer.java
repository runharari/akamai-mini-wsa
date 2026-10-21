package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.common.model.Severity;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;

@Component
@Profile({"processor", "default"})
public class DefaultThreatScorer implements ThreatScorer {

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
            case CRITICAL -> 40;
            case HIGH -> 30;
            case MEDIUM -> 20;
            case LOW -> 10;
        };
    }

    private int actionPoints(Action action) {
        return switch (action) {
            case DENY -> 20;
            case ALERT -> 10;
            case MONITOR -> 0;
        };
    }

    private boolean isSensitivePath(String path) {
        String lower = path.toLowerCase(Locale.ROOT);
        return SENSITIVE_PATHS.stream().anyMatch(lower::contains);
    }
}
