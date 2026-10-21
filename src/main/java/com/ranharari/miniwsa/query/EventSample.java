package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.GeoLocation;
import com.ranharari.miniwsa.common.model.Rule;

import java.time.Instant;

/** One stored event as returned by {@code GET /v1/events/samples}: the input fields plus the enriched ones. */
public record EventSample(
        String eventId,
        Instant timestamp,
        Instant receivedAt,
        long configId,
        String policyId,
        String clientIp,
        String hostname,
        String path,
        String method,
        Integer statusCode,
        String userAgent,
        Rule rule,
        Action action,
        GeoLocation geoLocation,
        Long requestSize,
        Long responseSize,
        String attackType,
        int threatScore) {
}
