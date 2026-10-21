package com.ranharari.miniwsa.ingest;

import com.ranharari.miniwsa.ingest.config.IngestProperties;
import com.ranharari.miniwsa.common.exception.Violation;
import com.ranharari.miniwsa.common.exception.InvalidRequestException;
import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.SecurityEvent;
import com.ranharari.miniwsa.support.TestEvents;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.cfg.EnumFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.tuple;

class JsonEventRequestParserTest {

    private static final ValidatorFactory VALIDATORS = Validation.buildDefaultValidatorFactory();

    private final JsonMapper jsonMapper = JsonMapper.builder().enable(EnumFeature.FAIL_ON_NUMBERS_FOR_ENUMS).build();
    private final Validator validator = VALIDATORS.getValidator();
    private final JsonEventRequestParser parser =
            new JsonEventRequestParser(jsonMapper, validator, new IngestProperties(3, Duration.ofSeconds(1)));

    @AfterAll
    static void closeValidators() {
        VALIDATORS.close();
    }

    @Test
    void parsesSingleObject() {
        List<SecurityEvent> events = parser.parse(json(TestEvents.VALID_JSON));

        assertThat(events).hasSize(1);
        SecurityEvent event = events.getFirst();
        assertThat(event.eventId()).isEqualTo("evt-00132");
        assertThat(event.timestamp()).isEqualTo(Instant.parse("2026-05-20T14:32:10Z"));
        assertThat(event.action()).isEqualTo(Action.DENY);
        assertThat(event.geoLocation().city()).isEqualTo("Beijing");
    }

    @Test
    void parsesArrayInOrder() {
        ArrayNode array = jsonMapper.createArrayNode();
        array.add(validEvent("a"));
        array.add(validEvent("b"));

        assertThat(parser.parse(array)).extracting(SecurityEvent::eventId).containsExactly("a", "b");
    }

    @Test
    void acceptsEventWithOnlyRequiredFields() {
        ObjectNode minimal = jsonMapper.createObjectNode();
        minimal.put("eventId", "e1").put("timestamp", "2026-05-20T14:32:10Z").put("configId", 1)
                .put("clientIp", "1.2.3.4").put("path", "/").put("method", "GET").put("action", "MONITOR");
        minimal.putObject("rule").put("id", "1").put("severity", "LOW").put("category", "BOT");

        SecurityEvent event = parser.parse(minimal).getFirst();

        assertThat(event.statusCode()).isNull();
        assertThat(event.geoLocation()).isNull();
    }

    @Test
    void reportsMissingRequiredFieldsWithNestedPath() {
        ObjectNode event = validEvent("e1");
        event.remove("clientIp");
        ((ObjectNode) event.get("rule")).remove("severity");

        assertThat(errorsFor(event)).containsExactly(
                Violation.ofEvent(0, "clientIp", "must not be blank"),
                Violation.ofEvent(0, "rule.severity", "must not be null"));
    }

    @Test
    void reportsUnknownEnumValueWithAllowedValues() {
        ObjectNode event = validEvent("e1");
        ((ObjectNode) event.get("rule")).put("category", "PHISHING");

        assertThat(errorsFor(event)).containsExactly(Violation.ofEvent(0, "rule.category",
                "must be one of INJECTION, XSS, PROTOCOL_VIOLATION, DATA_LEAKAGE, BOT, DOS, RATE_LIMIT"));
    }

    @Test
    void rejectsLowercaseAndNumericEnums() {
        assertThat(errorsFor(validEvent("e1").put("action", "deny"))).extracting(Violation::field).containsExactly("action");
        assertThat(errorsFor(validEvent("e1").put("action", 0))).extracting(Violation::field).containsExactly("action");
    }

    @Test
    void rejectsTimestampWithoutZoneOrAsNumber() {
        String message = "must be an ISO-8601 timestamp with a zone, e.g. 2026-05-20T14:32:10Z";

        assertThat(errorsFor(validEvent("e1").put("timestamp", "2026-05-20T14:32:10")))
                .containsExactly(Violation.ofEvent(0, "timestamp", message));
        assertThat(errorsFor(validEvent("e1").put("timestamp", 1747751530)))
                .containsExactly(Violation.ofEvent(0, "timestamp", message));
    }

    @Test
    void reportsErrorsOfEveryBadEventByIndex() {
        ArrayNode array = jsonMapper.createArrayNode();
        array.add(validEvent("ok"));
        array.add(validEvent("bad1").put("action", "BLOCK"));
        array.add(validEvent("bad2").put("path", ""));

        InvalidRequestException e = catchThrowableOfType(InvalidRequestException.class, () -> parser.parse(array));

        assertThat(e.violations()).extracting(Violation::index, Violation::field)
                .containsExactly(tuple(1, "action"), tuple(2, "path"));
    }

    @Test
    void rejectsEmptyArrayOversizedBatchAndNonObjects() {
        ArrayNode tooMany = jsonMapper.createArrayNode();
        for (int i = 0; i < 4; i++) {
            tooMany.add(validEvent("e" + i));
        }

        assertThat(messageOf(jsonMapper.createArrayNode())).isEqualTo("batch must contain at least one event");
        assertThat(messageOf(tooMany)).isEqualTo("batch has 4 events, the maximum is 3");
        assertThat(messageOf(jsonMapper.getNodeFactory().stringNode("hello"))).isEqualTo("body must be a JSON object or array");
        assertThat(errorsFor(jsonMapper.createArrayNode().add(42)))
                .containsExactly(Violation.ofEvent(0, null, "event must be a JSON object"));
    }

    private ObjectNode validEvent(String eventId) {
        ObjectNode node = (ObjectNode) json(TestEvents.VALID_JSON);
        return node.put("eventId", eventId);
    }

    private List<Violation> errorsFor(JsonNode body) {
        return catchThrowableOfType(InvalidRequestException.class, () -> parser.parse(body)).violations();
    }

    private String messageOf(JsonNode body) {
        return errorsFor(body).getFirst().message();
    }

    private JsonNode json(String text) {
        return jsonMapper.readTree(text);
    }
}
