package com.ranharari.miniwsa.enrich.config;

import com.ranharari.miniwsa.enrich.AttackClassifier;
import com.ranharari.miniwsa.enrich.DefaultAttackClassifier;
import com.ranharari.miniwsa.enrich.DefaultEnricher;
import com.ranharari.miniwsa.enrich.DefaultRepeatOffenderWindow;
import com.ranharari.miniwsa.enrich.DefaultThreatScorer;
import com.ranharari.miniwsa.enrich.Enricher;
import com.ranharari.miniwsa.enrich.RepeatOffenderWindow;
import com.ranharari.miniwsa.enrich.ThreatScorer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/** The scoring logic. Plain classes: the processor's Kafka Streams code calls them. */
@Configuration
@Profile({"processor", "default"})
public class EnrichConfig {

    @Bean
    AttackClassifier attackClassifier() {
        return new DefaultAttackClassifier();
    }

    @Bean
    ThreatScorer threatScorer() {
        return new DefaultThreatScorer();
    }

    @Bean
    Enricher enricher(AttackClassifier classifier, ThreatScorer scorer) {
        return new DefaultEnricher(classifier, scorer);
    }

    @Bean
    RepeatOffenderWindow repeatOffenderWindow() {
        return new DefaultRepeatOffenderWindow();
    }
}
