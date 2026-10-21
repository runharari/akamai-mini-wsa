package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.common.exception.Violation;
import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import jakarta.validation.Validator;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.exc.InvalidFormatException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Parses each event on its own, so every error can name its event index and field.
 * Three kinds of errors: Jackson can't convert a value (bad enum, bad timestamp), Bean Validation
 * finds a missing field or a number out of range, or the timestamp is outside the accepted range.
 * All errors of all events are collected before rejecting the request.
 */
public class JsonEventRequestParser implements EventRequestParser {

    private final JsonMapper jsonMapper;
    private final Validator validator;
    private final RequestLimits limits;
    private final Clock clock;

    public JsonEventRequestParser(JsonMapper jsonMapper, Validator validator, RequestLimits limits, Clock clock) {
        this.jsonMapper = jsonMapper;
        this.validator = validator;
        this.limits = limits;
        this.clock = clock;
    }

    @Override
    public List<SecurityEvent> parse(JsonNode body) {
        List<JsonNode> nodes = toEventNodes(body);

        Instant now = clock.instant().truncatedTo(ChronoUnit.SECONDS); // whole seconds read better in errors
        List<SecurityEvent> events = new ArrayList<>();
        List<Violation> errors = new ArrayList<>();
        for (int index = 0; index < nodes.size(); index++) {
            List<Violation> eventErrors = new ArrayList<>();
            SecurityEvent event = convert(index, nodes.get(index), eventErrors);
            if (event != null) {
                eventErrors.addAll(validate(index, event, now));
            }
            if (eventErrors.isEmpty()) {
                events.add(event);
            } else {
                errors.addAll(eventErrors);
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidRequestException(errors);
        }
        return events;
    }

    private List<JsonNode> toEventNodes(JsonNode body) {
        if (body == null || !(body.isObject() || body.isArray())) {
            throw new InvalidRequestException(Violation.ofParam(null, "body must be a JSON object or array"));
        }
        if (body.isObject()) {
            return List.of(body);
        }
        List<JsonNode> nodes = List.copyOf(body.values());
        if (nodes.isEmpty()) {
            throw new InvalidRequestException(Violation.ofParam(null, "batch must contain at least one event"));
        }
        if (nodes.size() > limits.maxBatchSize()) {
            throw new InvalidRequestException(Violation.ofParam(null,
                    "batch has " + nodes.size() + " events, the maximum is " + limits.maxBatchSize()));
        }
        return nodes;
    }

    /** Returns the event, or null after adding the conversion error. */
    private SecurityEvent convert(int index, JsonNode node, List<Violation> errors) {
        if (!node.isObject()) {
            errors.add(Violation.ofEvent(index, null, "event must be a JSON object"));
            return null;
        }
        // Jackson would also accept a number here (epoch seconds); the brief asks for ISO-8601.
        JsonNode timestamp = node.get("timestamp");
        if (timestamp != null && !timestamp.isNull() && !timestamp.isString()) {
            errors.add(Violation.ofEvent(index, "timestamp", TIMESTAMP_MESSAGE));
            return null;
        }
        try {
            return jsonMapper.treeToValue(node, SecurityEvent.class);
        } catch (JacksonException e) {
            errors.add(Violation.ofEvent(index, fieldPath(e), conversionMessage(e)));
            return null;
        }
    }

    private List<Violation> validate(int index, SecurityEvent event, Instant now) {
        List<Violation> violations = validator.validate(event).stream()
                .map(violation -> Violation.ofEvent(index, violation.getPropertyPath().toString(), violation.getMessage()))
                .sorted((a, b) -> a.field().compareTo(b.field()))
                .toList();
        if (!violations.isEmpty()) {
            return violations; // the timestamp may be missing, so check its range only on an otherwise valid event
        }
        return checkTimestampRange(index, event, now);
    }

    /**
     * A timestamp far in the future or past would break the pipeline later: Kafka refuses messages
     * dated more than an hour ahead, and Kafka Streams drops events before 1970.
     */
    private List<Violation> checkTimestampRange(int index, SecurityEvent event, Instant now) {
        Instant earliest = now.minus(limits.maxEventAge());
        Instant latest = now.plus(limits.maxClockSkew());
        if (event.timestamp().isBefore(earliest) || event.timestamp().isAfter(latest)) {
            return List.of(Violation.ofEvent(index, "timestamp", "must be between " + earliest + " and " + latest));
        }
        return List.of();
    }

    private static final String TIMESTAMP_MESSAGE = "must be an ISO-8601 timestamp with a zone, e.g. 2026-05-20T14:32:10Z";

    private static String fieldPath(JacksonException e) {
        return e.getPath().stream()
                .map(JacksonException.Reference::getPropertyName)
                .filter(name -> name != null)
                .collect(Collectors.joining("."));
    }

    private static String conversionMessage(JacksonException e) {
        if (e instanceof InvalidFormatException invalid) {
            Class<?> type = invalid.getTargetType();
            if (type != null && type.isEnum()) {
                String allowed = Arrays.stream(type.getEnumConstants()).map(Object::toString)
                        .collect(Collectors.joining(", "));
                return "must be one of " + allowed;
            }
            if (type == Instant.class) {
                return TIMESTAMP_MESSAGE;
            }
        }
        return "has an invalid value";
    }
}
