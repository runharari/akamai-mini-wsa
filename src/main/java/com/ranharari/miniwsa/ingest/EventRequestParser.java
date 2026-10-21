package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import tools.jackson.databind.JsonNode;

import java.util.List;

/** Turns an ingest request body (one event or an array) into validated events. */
public interface EventRequestParser {

    /**
     * @throws InvalidRequestException listing every problem, by event index and field, if any event is invalid
     */
    List<SecurityEvent> parse(JsonNode body);
}
