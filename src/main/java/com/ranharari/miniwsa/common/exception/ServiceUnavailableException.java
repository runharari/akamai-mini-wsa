package com.ranharari.miniwsa.common.exception;

/** A dependency is down or not ready yet; the client should retry later (503). */
public class ServiceUnavailableException extends RuntimeException {

    public ServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
