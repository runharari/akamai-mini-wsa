package com.ranharari.miniwsa.support;

import com.ranharari.miniwsa.common.event.Action;
import com.ranharari.miniwsa.common.event.Category;
import com.ranharari.miniwsa.common.event.EnrichedEvent;
import com.ranharari.miniwsa.common.event.GeoLocation;
import com.ranharari.miniwsa.common.event.Rule;
import com.ranharari.miniwsa.common.event.SecurityEvent;
import com.ranharari.miniwsa.common.event.Severity;

import java.time.Instant;

/** Ready-made events and JSON for tests. */
public final class TestEvents {

    public static final String VALID_JSON = """
            {
              "eventId": "evt-00132",
              "timestamp": "2026-05-20T14:32:10Z",
              "configId": 14227,
              "policyId": "pol_web1",
              "clientIp": "203.0.113.42",
              "hostname": "www.example.com",
              "path": "/api/v1/login",
              "method": "POST",
              "statusCode": 403,
              "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
              "rule": {
                "id": "950001",
                "name": "SQL_INJECTION",
                "message": "SQL Injection Attack Detected",
                "severity": "CRITICAL",
                "category": "INJECTION"
              },
              "action": "DENY",
              "geoLocation": { "country": "CN", "city": "Beijing" },
              "requestSize": 1024,
              "responseSize": 256
            }
            """;

    private TestEvents() {
    }

    public static SecurityEvent event(String eventId, String clientIp, Instant timestamp) {
        return new SecurityEvent(eventId, timestamp, 14227L, "pol_web1", clientIp, "www.example.com",
                "/api/v1/login", "POST", 403, "Mozilla/5.0",
                new Rule("950001", "SQL_INJECTION", "SQL Injection Attack Detected", Severity.CRITICAL, Category.INJECTION),
                Action.DENY, new GeoLocation("CN", "Beijing"), 1024L, 256L, null);
    }

    public static EnrichedEvent enriched(String eventId) {
        SecurityEvent event = event(eventId, "203.0.113.42", Instant.parse("2026-05-20T14:32:10Z"))
                .withReceivedAt(Instant.parse("2026-05-20T14:32:11Z"));
        return new EnrichedEvent(event, "SQL/Command Injection", 75);
    }
}
