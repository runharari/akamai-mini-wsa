package com.ranharari.miniwsa.common.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;

/**
 * One security event (DLR) as sent by the edge.
 * {@code receivedAt} is always set by the server; a value sent by the client is overwritten.
 * Number limits match the ClickHouse column types (UInt16, UInt32), so a stored value is never cut off.
 */
public record SecurityEvent(
        @NotBlank String eventId,
        @NotNull Instant timestamp,
        @NotNull @PositiveOrZero @Max(UINT32_MAX) Long configId,
        String policyId,
        @NotBlank String clientIp,
        String hostname,
        @NotBlank String path,
        @NotBlank String method,
        @PositiveOrZero @Max(65_535) Integer statusCode,
        String userAgent,
        @NotNull @Valid Rule rule,
        @NotNull Action action,
        @Valid GeoLocation geoLocation,
        @PositiveOrZero @Max(UINT32_MAX) Long requestSize,
        @PositiveOrZero @Max(UINT32_MAX) Long responseSize,
        Instant receivedAt) {

    private static final long UINT32_MAX = 4_294_967_295L;

    public SecurityEvent withReceivedAt(Instant receivedAt) {
        return new SecurityEvent(eventId, timestamp, configId, policyId, clientIp, hostname, path, method,
                statusCode, userAgent, rule, action, geoLocation, requestSize, responseSize, receivedAt);
    }
}
