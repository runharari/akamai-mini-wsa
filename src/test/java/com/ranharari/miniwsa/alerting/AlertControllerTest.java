package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.alerting.AlertEvaluation.FiringAlert;
import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;
import com.ranharari.miniwsa.common.model.Category;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AlertController.class)
class AlertControllerTest {

    private static final AlertRule RULE = new AlertRule("r1", "SQLi burst", Category.INJECTION, 14227L, 100, 5);

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertService alertService;

    @Test
    void defineReturns201WithTheSavedRule() throws Exception {
        AlertRuleRequest request = new AlertRuleRequest("SQLi burst", Category.INJECTION, 14227L, 100L, 5);
        when(alertService.define(request)).thenReturn(RULE);

        mockMvc.perform(post("/v1/alerts/define").contentType(MediaType.APPLICATION_JSON).content("""
                        {"name":"SQLi burst","category":"INJECTION","configId":14227,"threshold":100,"windowMinutes":5}"""))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value("r1"))
                .andExpect(jsonPath("$.category").value("INJECTION"))
                .andExpect(jsonPath("$.windowMinutes").value(5));
        verify(alertService).define(request);
    }

    @Test
    void defineRejectsMissingAndOutOfRangeFieldsWithoutSavingAnything() throws Exception {
        mockMvc.perform(post("/v1/alerts/define").contentType(MediaType.APPLICATION_JSON).content("""
                        {"category":"INJECTION","windowMinutes":2000}"""))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("""
                        {"errors":[{"field":"threshold","message":"must not be null"},
                                   {"field":"windowMinutes","message":"must be less than or equal to 1440"}]}""", true));
        verifyNoInteractions(alertService);
    }

    @Test
    void defineRejectsAnUnknownCategory() throws Exception {
        mockMvc.perform(post("/v1/alerts/define").contentType(MediaType.APPLICATION_JSON).content("""
                        {"category":"PHISHING","threshold":1,"windowMinutes":5}"""))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(alertService);
    }

    @Test
    void evaluateReturnsTheFiringRules() throws Exception {
        when(alertService.evaluate()).thenReturn(new AlertEvaluation(Instant.parse("2026-05-20T14:10:30Z"), 3,
                List.of(FiringAlert.of(RULE, 143))));

        mockMvc.perform(get("/v1/alerts/evaluate"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.evaluatedAt").value("2026-05-20T14:10:30Z"))
                .andExpect(jsonPath("$.rulesChecked").value(3))
                .andExpect(jsonPath("$.firing[0].ruleId").value("r1"))
                .andExpect(jsonPath("$.firing[0].count").value(143))
                .andExpect(jsonPath("$.firing[0].threshold").value(100));
    }

    @Test
    void evaluateReturns503WhileAlertingIsStarting() throws Exception {
        when(alertService.evaluate()).thenThrow(new ServiceUnavailableException("Alerting is starting", null));

        mockMvc.perform(get("/v1/alerts/evaluate"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "5"))
                .andExpect(jsonPath("$.errors[0].message").value("Alerting is starting"));
    }
}
