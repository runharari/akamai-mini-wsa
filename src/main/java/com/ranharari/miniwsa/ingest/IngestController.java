package com.ranharari.miniwsa.ingest;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;

@RestController
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    /** Accepts one event or an array of events. 201 means every event is stored in Kafka. */
    @PostMapping("/v1/events/ingest")
    @ResponseStatus(HttpStatus.CREATED)
    public IngestResponse ingest(@RequestBody JsonNode body) {
        return new IngestResponse(ingestService.ingest(body));
    }

    public record IngestResponse(int ingested) {
    }
}
