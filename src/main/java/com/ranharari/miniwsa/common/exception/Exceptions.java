package com.ranharari.miniwsa.common.exception;

/** Small helpers for exceptions. */
public final class Exceptions {

    private Exceptions() {
    }

    /** The message of the deepest cause, e.g. the database's own error under our wrapper exceptions. */
    public static String rootCauseMessage(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}
