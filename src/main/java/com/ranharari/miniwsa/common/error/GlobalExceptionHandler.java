package com.ranharari.miniwsa.common.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.ErrorResponse;
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

    @ExceptionHandler(InvalidRequestException.class)
    ResponseEntity<ErrorBody> invalidRequest(InvalidRequestException e) {
        // The client's problem, not ours: DEBUG, not WARN.
        log.debug("Rejected request errors={}", e.errors());
        return ResponseEntity.badRequest().body(new ErrorBody(e.errors()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ErrorBody> unreadableBody(HttpMessageNotReadableException e) {
        log.debug("Unreadable request body: {}", e.getMessage());
        return ResponseEntity.badRequest().body(ErrorBody.of("request body is missing or not valid JSON"));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    ResponseEntity<ErrorBody> badParameter(MethodArgumentTypeMismatchException e) {
        log.debug("Bad parameter name={} value={}", e.getName(), e.getValue());
        FieldError error = FieldError.ofParam(e.getName(), "invalid value '" + e.getValue() + "'");
        return ResponseEntity.badRequest().body(new ErrorBody(List.of(error)));
    }

    @ExceptionHandler(PublishFailedException.class)
    ResponseEntity<ErrorBody> publishFailed(PublishFailedException e) {
        log.warn("Ingest unavailable: {}", e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "5")
                .body(ErrorBody.of("ingest temporarily unavailable, retry later"));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ErrorBody> unexpected(Exception e) {
        // Spring's own web errors (404, 405, 415, missing parameter, ...) keep their status.
        if (e instanceof ErrorResponse springError) {
            log.debug("Web error status={} message={}", springError.getStatusCode(), e.getMessage());
            String detail = springError.getBody().getDetail();
            return ResponseEntity.status(springError.getStatusCode())
                    .body(ErrorBody.of(detail != null ? detail : e.getMessage()));
        }
        log.error("Unexpected error", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorBody.of("internal error"));
    }
}
