package com.ranharari.miniwsa.alerting;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AlertController {

    private final AlertService alertService;

    public AlertController(AlertService alertService) {
        this.alertService = alertService;
    }

    /** Saves a rule: "more than {threshold} events of {category} within {windowMinutes} minutes". */
    @PostMapping("/v1/alerts/define")
    @ResponseStatus(HttpStatus.CREATED)
    public AlertRule define(@Valid @RequestBody AlertRuleRequest request) {
        return alertService.define(request);
    }

    /** The rules that are firing right now. */
    @GetMapping("/v1/alerts/evaluate")
    public AlertEvaluation evaluate() {
        return alertService.evaluate();
    }
}
