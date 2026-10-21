package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.Category;

/** Maps a rule category to the human-readable attack type. */
public interface AttackClassifier {

    /** e.g. INJECTION → "SQL/Command Injection" */
    String attackType(Category category);
}
