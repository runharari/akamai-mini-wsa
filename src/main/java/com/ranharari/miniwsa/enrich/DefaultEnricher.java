package com.ranharari.miniwsa.enrich;

import com.ranharari.miniwsa.common.model.EnrichedEvent;
import com.ranharari.miniwsa.common.model.SecurityEvent;

public class DefaultEnricher implements Enricher {

    private final AttackClassifier classifier;
    private final ThreatScorer scorer;

    public DefaultEnricher(AttackClassifier classifier, ThreatScorer scorer) {
        this.classifier = classifier;
        this.scorer = scorer;
    }

    @Override
    public EnrichedEvent enrich(SecurityEvent event, boolean repeatOffender) {
        return new EnrichedEvent(
                event,
                classifier.attackType(event.rule().category()),
                scorer.score(event, repeatOffender));
    }
}
