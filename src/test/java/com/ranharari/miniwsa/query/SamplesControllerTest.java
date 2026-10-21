package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;
import com.ranharari.miniwsa.common.model.GeoLocation;
import com.ranharari.miniwsa.common.model.Rule;
import com.ranharari.miniwsa.common.model.Severity;
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
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** The real {@link DefaultTimeRangeResolver} runs here (with a fixed clock); only the service is mocked. */
@WebMvcTest(SamplesController.class)
@Import({SamplesControllerTest.FixedClock.class, DefaultTimeRangeResolver.class})
class SamplesControllerTest {

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
    private SamplesService samplesService;

    @Test
    void usesTheDefaultsAndReturnsTheFlatEventShape() throws Exception {
        TimeRange expected = new TimeRange(Instant.parse("2026-05-20T10:00:00Z"), NOW);
        SampleFilter filter = new SampleFilter(null, expected, null, null);
        EventSample sample = new EventSample("evt-00132", Instant.parse("2026-05-20T14:32:10Z"),
                Instant.parse("2026-05-20T14:32:11Z"), 14227L, "pol_web1", "203.0.113.42", "www.example.com",
                "/api/v1/login", "POST", 403, "Mozilla/5.0",
                new Rule("950001", "SQL_INJECTION", "SQL Injection Attack Detected", Severity.CRITICAL,
                        Category.INJECTION),
                Action.DENY, new GeoLocation("CN", "Beijing"), 1024L, 256L, "SQL/Command Injection", 75);
        when(samplesService.samples(filter, 20, 0)).thenReturn(new SamplesPage(57, 20, 0, List.of(sample)));

        mockMvc.perform(get("/v1/events/samples"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(57))
                .andExpect(jsonPath("$.limit").value(20))
                .andExpect(jsonPath("$.offset").value(0))
                .andExpect(jsonPath("$.events[0].eventId").value("evt-00132"))
                .andExpect(jsonPath("$.events[0].timestamp").value("2026-05-20T14:32:10Z"))
                .andExpect(jsonPath("$.events[0].receivedAt").value("2026-05-20T14:32:11Z"))
                .andExpect(jsonPath("$.events[0].configId").value(14227))
                .andExpect(jsonPath("$.events[0].rule.category").value("INJECTION"))
                .andExpect(jsonPath("$.events[0].action").value("DENY"))
                .andExpect(jsonPath("$.events[0].geoLocation.country").value("CN"))
                .andExpect(jsonPath("$.events[0].attackType").value("SQL/Command Injection"))
                .andExpect(jsonPath("$.events[0].threatScore").value(75));
        verify(samplesService).samples(filter, 20, 0);
    }

    @Test
    void passesAllFiltersAndPagingToTheService() throws Exception {
        TimeRange range = new TimeRange(Instant.parse("2026-05-01T00:00:00Z"), Instant.parse("2026-05-02T00:00:00Z"));
        SampleFilter filter = new SampleFilter(14227L, range, Category.BOT, Action.ALERT);
        when(samplesService.samples(filter, 100, 5)).thenReturn(new SamplesPage(0, 100, 5, List.of()));

        mockMvc.perform(get("/v1/events/samples")
                        .param("configId", "14227")
                        .param("from", "2026-05-01T00:00:00Z")
                        .param("to", "2026-05-02T00:00:00Z")
                        .param("category", "BOT")
                        .param("action", "ALERT")
                        .param("limit", "100")
                        .param("offset", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.limit").value(100))
                .andExpect(jsonPath("$.offset").value(5))
                .andExpect(jsonPath("$.events").isEmpty());
        verify(samplesService).samples(filter, 100, 5);
    }

    @Test
    void returns400ForALimitBelowOne() throws Exception {
        mockMvc.perform(get("/v1/events/samples").param("limit", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{\"errors\":[{\"field\":\"limit\",\"message\":\"must be between 1 and 100\"}]}", true));
        verifyNoInteractions(samplesService);
    }

    @Test
    void returns400ForALimitAbove100() throws Exception {
        mockMvc.perform(get("/v1/events/samples").param("limit", "101"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{\"errors\":[{\"field\":\"limit\",\"message\":\"must be between 1 and 100\"}]}", true));
        verifyNoInteractions(samplesService);
    }

    @Test
    void returns400ForANegativeOffset() throws Exception {
        mockMvc.perform(get("/v1/events/samples").param("offset", "-1"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{\"errors\":[{\"field\":\"offset\",\"message\":\"must not be negative\"}]}", true));
        verifyNoInteractions(samplesService);
    }

    @Test
    void returns400ForAnUnknownCategory() throws Exception {
        mockMvc.perform(get("/v1/events/samples").param("category", "PHISHING"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("category"));
        verifyNoInteractions(samplesService);
    }

    @Test
    void returns400ForAnActionInTheWrongCase() throws Exception {
        mockMvc.perform(get("/v1/events/samples").param("action", "deny"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("action"));
        verifyNoInteractions(samplesService);
    }
}
