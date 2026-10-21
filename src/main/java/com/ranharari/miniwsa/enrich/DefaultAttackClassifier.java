package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.Category;

public class DefaultAttackClassifier implements AttackClassifier {

    @Override
    public String attackType(Category category) {
        // No default branch: adding a Category value fails to compile until it is mapped here.
        return switch (category) {
            case INJECTION -> "SQL/Command Injection";
            case XSS -> "Cross-Site Scripting";
            case PROTOCOL_VIOLATION -> "Protocol Anomaly";
            case DATA_LEAKAGE -> "Data Exfiltration";
            case BOT -> "Bot Activity";
            case DOS -> "Denial of Service";
            case RATE_LIMIT -> "Rate Limiting";
        };
    }
}
