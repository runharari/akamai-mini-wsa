package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.Category;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultAttackClassifierTest {

    private final DefaultAttackClassifier classifier = new DefaultAttackClassifier();

    @ParameterizedTest
    @CsvSource({
            "INJECTION, SQL/Command Injection",
            "XSS, Cross-Site Scripting",
            "PROTOCOL_VIOLATION, Protocol Anomaly",
            "DATA_LEAKAGE, Data Exfiltration",
            "BOT, Bot Activity",
            "DOS, Denial of Service",
            "RATE_LIMIT, Rate Limiting"
    })
    void mapsCategoryToAttackType(Category category, String expected) {
        assertThat(classifier.attackType(category)).isEqualTo(expected);
    }

    @Test
    void everyCategoryMapsToANonBlankType() {
        for (Category category : Category.values()) {
            assertThat(classifier.attackType(category)).as(category.name()).isNotBlank();
        }
    }
}
