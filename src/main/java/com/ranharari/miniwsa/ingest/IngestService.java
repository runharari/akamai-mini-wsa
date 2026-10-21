package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.exception.PublishFailedException;
import tools.jackson.databind.JsonNode;

public interface IngestService {

    /**
     * Validates the body (one event or an array), stamps receivedAt and publishes the events.
     *
     * @return how many events were accepted
     * @throws InvalidRequestException if any event is invalid (nothing is published)
     * @throws PublishFailedException  if the events could not be published
     */
    int ingest(JsonNode body);
}
