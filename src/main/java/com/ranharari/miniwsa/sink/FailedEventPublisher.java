package com.ranharari.miniwsa.sink;

import com.ranharari.miniwsa.common.exception.PublishFailedException;
import com.ranharari.miniwsa.common.model.SecurityEvent;

/** Parks an event that can't be stored, so it doesn't block the events behind it. */
public interface FailedEventPublisher {

    /**
     * Returns only after the event is durably stored on the failed topic.
     *
     * @throws PublishFailedException if it could not be stored
     */
    void publish(SecurityEvent event, Exception cause);
}
