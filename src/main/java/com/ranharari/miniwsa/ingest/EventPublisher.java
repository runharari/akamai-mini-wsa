package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.error.PublishFailedException;
import com.ranharari.miniwsa.common.event.SecurityEvent;

import java.util.List;

/** Hands validated events to the pipeline. */
public interface EventPublisher {

    /**
     * Returns only after every event is durably stored (acknowledged).
     *
     * @throws PublishFailedException if not all events were acknowledged in time
     */
    void publish(List<SecurityEvent> events);
}
