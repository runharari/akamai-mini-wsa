package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.query.StatsSummary.AttackerStats;
import com.ranharari.miniwsa.query.StatsSummary.CategoryStats;
import com.ranharari.miniwsa.query.StatsSummary.PathStats;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The real {@link DefaultTimeRangeResolver} runs here (with a fixed clock); only the service is mocked. */
@WebMvcTest(StatsController.class)
@Import({StatsControllerTest.FixedClock.class, DefaultTimeRangeResolver.class})
class StatsControllerTest {

    private static final Instant NOW = Instant.parse("2026-05-21T10:00:00Z");

    @TestConfiguration
    static class FixedClock {
        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private StatsService statsService;

    @Test
    void usesTheLast24HoursAndAllConfigsByDefaultAndReturnsTheBriefShape() throws Exception {
        TimeRange expected = new TimeRange(Instant.parse("2026-05-20T10:00:00Z"), NOW);
        Map<String, CategoryStats> byCategory = new LinkedHashMap<>();
        byCategory.put("INJECTION", new CategoryStats(450, 72.3));
        byCategory.put("BOT", new CategoryStats(380, 45.1));
        Map<String, Long> byAction = new LinkedHashMap<>();
        byAction.put("DENY", 890L);
        byAction.put("ALERT", 433L);
        when(statsService.summary(null, expected)).thenReturn(new StatsSummary(null, expected, 1523, byCategory,
                byAction, List.of(new AttackerStats("203.0.113.42", 87, 81.2)),
                List.of(new PathStats("/api/v1/login", 234))));

        mockMvc.perform(get("/v1/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.configId").value((Object) null))
                .andExpect(jsonPath("$.timeRange.from").value("2026-05-20T10:00:00Z"))
                .andExpect(jsonPath("$.timeRange.to").value("2026-05-21T10:00:00Z"))
                .andExpect(jsonPath("$.totalEvents").value(1523))
                .andExpect(jsonPath("$.byCategory.INJECTION.count").value(450))
                .andExpect(jsonPath("$.byCategory.INJECTION.avgThreatScore").value(72.3))
                .andExpect(jsonPath("$.byAction.DENY").value(890))
                .andExpect(jsonPath("$.topAttackers[0].clientIp").value("203.0.113.42"))
                .andExpect(jsonPath("$.topAttackers[0].count").value(87))
                .andExpect(jsonPath("$.topAttackers[0].avgThreatScore").value(81.2))
                .andExpect(jsonPath("$.topTargetedPaths[0].path").value("/api/v1/login"))
                .andExpect(jsonPath("$.topTargetedPaths[0].count").value(234));
        verify(statsService).summary(null, expected);
    }

    @Test
    void passesConfigIdAndExplicitRangeToTheService() throws Exception {
        TimeRange range = new TimeRange(Instant.parse("2026-05-01T00:00:00Z"), Instant.parse("2026-05-02T00:00:00Z"));
        when(statsService.summary(14227L, range)).thenReturn(
                new StatsSummary(14227L, range, 0, Map.of(), Map.of(), List.of(), List.of()));

        mockMvc.perform(get("/v1/stats/summary")
                        .param("configId", "14227")
                        .param("from", "2026-05-01T00:00:00Z")
                        .param("to", "2026-05-02T00:00:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.configId").value(14227))
                .andExpect(jsonPath("$.totalEvents").value(0));
        verify(statsService).summary(14227L, range);
    }

    @Test
    void returns400WhenFromIsNotBeforeTo() throws Exception {
        mockMvc.perform(get("/v1/stats/summary")
                        .param("from", "2026-05-02T00:00:00Z")
                        .param("to", "2026-05-01T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{\"errors\":[{\"field\":\"from\",\"message\":\"must be before to\"}]}", true));
        verifyNoInteractions(statsService);
    }

    @Test
    void returns400ForAnUnparsableTime() throws Exception {
        mockMvc.perform(get("/v1/stats/summary").param("from", "yesterday"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("from"));
        verifyNoInteractions(statsService);
    }

    @Test
    void returns400ForANonNumericConfigId() throws Exception {
        mockMvc.perform(get("/v1/stats/summary").param("configId", "abc"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("configId"));
        verifyNoInteractions(statsService);
    }
}
