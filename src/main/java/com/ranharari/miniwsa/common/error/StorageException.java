package com.ranharari.miniwsa.common.error;

/** A ClickHouse read or write failed. */
public class StorageException extends RuntimeException {

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
