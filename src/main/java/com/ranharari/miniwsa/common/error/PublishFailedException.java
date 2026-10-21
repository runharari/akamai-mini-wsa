package com.ranharari.miniwsa.common.error;

/** Kafka didn't acknowledge the events in time. Mapped to 503 so the client retries later. */
public class PublishFailedException extends RuntimeException {

    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
