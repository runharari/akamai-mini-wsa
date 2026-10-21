package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.exception.Violation;
import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.exception.PublishFailedException;
import com.ranharari.miniwsa.common.web.config.WebConfig;
import com.ranharari.miniwsa.support.TestEvents;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest
@Import({IngestController.class, WebConfig.class})
class IngestControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private IngestService ingestService;

    @Test
    void returns201WithAcceptedCount() throws Exception {
        when(ingestService.ingest(any())).thenReturn(1);

        mockMvc.perform(post("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON).content(TestEvents.VALID_JSON))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.ingested").value(1))
                .andExpect(header().exists("X-Request-Id"));
    }

    @Test
    void returns400WithPerEventFieldErrors() throws Exception {
        when(ingestService.ingest(any())).thenThrow(new InvalidRequestException(List.of(
                Violation.ofEvent(0, "rule.severity", "must not be null"),
                Violation.ofEvent(2, "action", "must be one of DENY, ALERT, MONITOR"))));

        mockMvc.perform(post("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON).content("[{},{},{}]"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].index").value(0))
                .andExpect(jsonPath("$.errors[0].field").value("rule.severity"))
                .andExpect(jsonPath("$.errors[1].index").value(2))
                .andExpect(jsonPath("$.errors[1].message").value("must be one of DENY, ALERT, MONITOR"));
    }

    @Test
    void returns503WithRetryAfterWhenKafkaIsUnavailable() throws Exception {
        when(ingestService.ingest(any())).thenThrow(new PublishFailedException("timeout", new RuntimeException()));

        mockMvc.perform(post("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON).content(TestEvents.VALID_JSON))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "5"))
                .andExpect(jsonPath("$.errors[0].message").value("ingest temporarily unavailable, retry later"));
    }

    @Test
    void returns400ForMalformedJsonWithoutCallingTheService() throws Exception {
        mockMvc.perform(post("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON).content("{oops"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].message").value("request body is missing or not valid JSON"));

        verifyNoInteractions(ingestService);
    }

    @Test
    void returns500WithoutLeakingDetailsOnUnexpectedErrors() throws Exception {
        when(ingestService.ingest(any())).thenThrow(new IllegalStateException("secret internals"));

        mockMvc.perform(post("/v1/events/ingest").contentType(MediaType.APPLICATION_JSON).content(TestEvents.VALID_JSON))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.errors[0].message").value("internal error"));
    }
}
