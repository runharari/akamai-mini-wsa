package com.ranharari.miniwsa.common.web;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.exception.PublishFailedException;
import com.ranharari.miniwsa.common.exception.ServiceUnavailableException;
import com.ranharari.miniwsa.common.exception.Violation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;

/**
 * Turns exceptions into the JSON error body. This is the one place that logs request failures,
 * so the layers below throw and don't log.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /** How long a client should wait before retrying a 503. */
    private static final String RETRY_AFTER_SECONDS = "5";

    @ExceptionHandler(InvalidRequestException.class)
    ResponseEntity<ApiErrorResponse> invalidRequest(InvalidRequestException e) {
        // The client's problem, not ours: DEBUG, not WARN.
        log.debug("Rejected request errors={}", e.violations());
        return ResponseEntity.badRequest().body(new ApiErrorResponse(e.violations()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ApiErrorResponse> unreadableBody(HttpMessageNotReadableException e) {
        log.debug("Unreadable request body: {}", e.getMessage());
        return ResponseEntity.badRequest().body(ApiErrorResponse.of("request body is missing or not valid JSON"));
    }

    /** A JSON body that failed Bean Validation (@Valid), e.g. an alert rule with a missing threshold. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ApiErrorResponse> invalidBody(MethodArgumentNotValidException e) {
        List<Violation> errors = e.getBindingResult().getFieldErrors().stream()
                .map(error -> Violation.ofParam(error.getField(), error.getDefaultMessage()))
                .sorted((a, b) -> a.field().compareTo(b.field()))
                .toList();
        log.debug("Rejected request body errors={}", errors);
        return ResponseEntity.badRequest().body(new ApiErrorResponse(errors));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<ApiErrorResponse> badParameter(MethodArgumentTypeMismatchException e) {
        log.debug("Bad parameter name={} value={}", e.getName(), e.getValue());
        Violation error = Violation.ofParam(e.getName(), "invalid value '" + e.getValue() + "'");
        return ResponseEntity.badRequest().body(new ApiErrorResponse(List.of(error)));
    }

    @ExceptionHandler(PublishFailedException.class)
    ResponseEntity<ApiErrorResponse> publishFailed(PublishFailedException e) {
        log.warn("Ingest unavailable: {}", e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(ApiErrorResponse.of("ingest temporarily unavailable, retry later"));
    }

    @ExceptionHandler(ServiceUnavailableException.class)
    ResponseEntity<ApiErrorResponse> unavailable(ServiceUnavailableException e) {
        log.warn("Service unavailable: {}", e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, RETRY_AFTER_SECONDS)
                .body(ApiErrorResponse.of(e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiErrorResponse> unexpected(Exception e) {
        // Spring's own web errors (404, 405, 415, missing parameter, ...) keep their status.
        if (e instanceof ErrorResponse springError) {
            log.debug("Web error status={} message={}", springError.getStatusCode(), e.getMessage());
            String detail = springError.getBody().getDetail();
            return ResponseEntity.status(springError.getStatusCode())
                    .body(ApiErrorResponse.of(detail != null ? detail : e.getMessage()));
        }
        log.error("Unexpected error", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ApiErrorResponse.of("internal error"));
    }
}
