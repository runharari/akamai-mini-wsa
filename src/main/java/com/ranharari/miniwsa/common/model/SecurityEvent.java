package com.ranharari.miniwsa.common.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;

/**
 * One security event (DLR) as sent by the edge.
 * {@code receivedAt} is always set by the server; a value sent by the client is overwritten.
 */
public record SecurityEvent(
        @NotBlank String eventId,
        @NotNull Instant timestamp,
        @NotNull @PositiveOrZero Long configId,
        String policyId,
        @NotBlank String clientIp,
        String hostname,
        @NotBlank String path,
        @NotBlank String method,
        Integer statusCode,
        String userAgent,
        @NotNull @Valid Rule rule,
        @NotNull Action action,
        @Valid GeoLocation geoLocation,
        Long requestSize,
        Long responseSize,
        Instant receivedAt) {

    public SecurityEvent withReceivedAt(Instant receivedAt) {
        return new SecurityEvent(eventId, timestamp, configId, policyId, clientIp, hostname, path, method,
                statusCode, userAgent, rule, action, geoLocation, requestSize, responseSize, receivedAt);
    }
}
