package com.ranharari.miniwsa;

import com.ranharari.miniwsa.support.PipelineContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The whole pipeline in one JVM (no profile = every role) against real Kafka and ClickHouse:
 * HTTP ingest → events.raw → Kafka Streams enrichment → events.enriched → sink → ClickHouse → query APIs.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PipelineIT {

    private static final String ATTACKER = "203.0.113.42";
    private static final String BOT = "198.51.100.7";

    /** A config id of its own, so rows of other tests never change the numbers. */
    private final long configId = ThreadLocalRandom.current().nextLong(1_000_000, 4_000_000_000L);
    /** An hour ago, so the timestamps pass ingest's range check; whole minutes keep them readable. */
    private final Instant start = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(Duration.ofHours(1));

    @LocalServerPort
    private int port;

    private RestTestClient http;

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry registry) {
        PipelineContainers.register(registry);
    }

    @BeforeEach
    void client() {
        http = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void ingestedEventsAreScoredStoredAndQueryable() {
        // An attack wave: 7 CRITICAL/DENY events on /admin, one minute apart. 40 + 20 + 15 = 75,
        // and the 7th has 6 earlier events within 10 minutes: +15 = 90. Plus one calm bot event: 10 + 0 = 10.
        List<String> events = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            events.add(event("wave-" + i, start.plus(Duration.ofMinutes(i)), ATTACKER, "/admin/users",
                    "CRITICAL", "INJECTION", "DENY"));
        }
        events.add(event("bot", start.plus(Duration.ofMinutes(30)), BOT, "/products", "LOW", "BOT", "MONITOR"));

        http.post().uri("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON)
                .body("[" + String.join(",", events) + "]")
                .exchange()
                .expectStatus().isCreated()
                .expectBody().jsonPath("$.ingested").isEqualTo(8);

        // Kafka Streams needs a few seconds to start and join its group; then rows arrive within about a second.
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() ->
                http.get().uri(summaryUri()).exchange()
                        .expectStatus().isOk()
                        .expectBody().jsonPath("$.totalEvents").isEqualTo(8));

        http.get().uri(summaryUri()).exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.configId").isEqualTo(configId)
                .jsonPath("$.byCategory.INJECTION.count").isEqualTo(7)
                .jsonPath("$.byCategory.INJECTION.avgThreatScore").isEqualTo(77.1) // (6 × 75 + 90) / 7
                .jsonPath("$.byCategory.BOT.avgThreatScore").isEqualTo(10.0)
                .jsonPath("$.byAction.DENY").isEqualTo(7)
                .jsonPath("$.byAction.MONITOR").isEqualTo(1)
                .jsonPath("$.topAttackers[0].clientIp").isEqualTo(ATTACKER)
                .jsonPath("$.topAttackers[0].count").isEqualTo(7)
                .jsonPath("$.topTargetedPaths[0].path").isEqualTo("/admin/users");

        http.get().uri(range("/v1/events/samples?category=INJECTION&limit=2")).exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.total").isEqualTo(7)
                .jsonPath("$.events.length()").isEqualTo(2)
                .jsonPath("$.events[0].eventId").isEqualTo(configId + "-wave-6")   // newest first
                .jsonPath("$.events[0].threatScore").isEqualTo(90)     // the repeat offender
                .jsonPath("$.events[0].attackType").isEqualTo("SQL/Command Injection")
                .jsonPath("$.events[1].eventId").isEqualTo(configId + "-wave-5")
                .jsonPath("$.events[1].threatScore").isEqualTo(75);
    }

    @Test
    void alertRuleFiresWhenAnAttackWaveCrossesItsThreshold() {
        // "More than 5 INJECTION events of this config within 2 hours."
        String ruleId = http.post().uri("/v1/alerts/define").contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"name":"SQLi burst","category":"INJECTION","configId":%d,"threshold":5,"windowMinutes":120}
                        """.formatted(configId))
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class).returnResult().getResponseBody()
                .replaceAll(".*\"id\":\"([^\"]+)\".*", "$1");

        List<String> wave = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            wave.add(event("alert-" + i, start.plus(Duration.ofMinutes(i)), ATTACKER, "/login", "HIGH", "INJECTION", "DENY"));
        }
        http.post().uri("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON)
                .body("[" + String.join(",", wave) + "]")
                .exchange()
                .expectStatus().isCreated();

        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() ->
                http.get().uri("/v1/alerts/evaluate").exchange()
                        .expectStatus().isOk()
                        .expectBody()
                        .jsonPath("$.firing[?(@.ruleId == '%s')].count".formatted(ruleId)).isEqualTo(List.of(6)));
    }

    @Test
    void invalidEventIsRejectedWithTheFieldThatIsWrong() {
        String bad = event("bad", start, ATTACKER, "/", "LOW", "BOT", "BLOCK");

        http.post().uri("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON).body(bad)
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.errors[0].field").isEqualTo("action")
                .jsonPath("$.errors[0].message").isEqualTo("must be one of DENY, ALERT, MONITOR");
    }

    @Test
    void pipelineMetricsAreExposedForPrometheus() {
        http.post().uri("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON)
                .body(event("metric", start, BOT, "/", "LOW", "BOT", "MONITOR"))
                .exchange()
                .expectStatus().isCreated();

        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).untilAsserted(() -> {
            String metrics = http.get().uri("/actuator/prometheus").exchange()
                    .expectStatus().isOk()
                    .expectBody(String.class).returnResult().getResponseBody();
            assertThat(metrics)
                    .contains("wsa_ingest_events_total{result=\"accepted\"")
                    .contains("wsa_enrich_events_total")
                    .contains("wsa_sink_batch_size_count");
        });
    }

    private String summaryUri() {
        return range("/v1/stats/summary?");
    }

    /** Adds this test's configId and a time range around its events. */
    private String range(String path) {
        String separator = path.endsWith("?") ? "" : "&";
        return path + separator + "configId=" + configId
                + "&from=" + start.minus(Duration.ofMinutes(1))
                + "&to=" + start.plus(Duration.ofHours(1));
    }

    /** Event ids get the configId as a prefix, so they are unique per run too. */
    private String event(String eventId, Instant timestamp, String clientIp, String path,
                         String severity, String category, String action) {
        return """
                {"eventId":"%s","timestamp":"%s","configId":%d,"clientIp":"%s","path":"%s","method":"POST",
                 "rule":{"id":"950001","severity":"%s","category":"%s"},"action":"%s"}
                """.formatted(configId + "-" + eventId, timestamp, configId, clientIp, path, severity, category, action);
    }
}
