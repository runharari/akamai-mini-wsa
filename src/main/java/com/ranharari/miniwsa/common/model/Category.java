package com.ranharari.miniwsa.common.model;

/** The rule category reported by the edge (rule.category). */
public enum Category {
    INJECTION,
    XSS,
    PROTOCOL_VIOLATION,
    DATA_LEAKAGE,
    BOT,
    DOS,
    RATE_LIMIT
}
