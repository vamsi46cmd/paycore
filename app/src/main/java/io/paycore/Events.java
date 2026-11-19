package io.paycore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;

/** Event envelope parsing and schema validation (schema version 1). */
public final class Events {
    private Events() {}

    /** Permanent: the message can never be processed, so it goes straight to the dead-letter topic without retries. */
    public static class InvalidEventException extends RuntimeException {
        public InvalidEventException(String m) { super(m); }
    }

    /** Temporary: infrastructure or provider trouble; the consumer retries with bounded exponential backoff. */
    public static class TransientProcessingException extends RuntimeException {
        public TransientProcessingException(String m) { super(m); }
    }

    public record Envelope(UUID eventId, String eventType, UUID aggregateId, String correlationId, JsonNode payload) {
        public UUID uuid(String field) {
            JsonNode n = payload.get(field);
            if (n == null || !n.isTextual()) {
                throw new InvalidEventException("payload." + field + " is required");
            }
            try {
                return UUID.fromString(n.asText());
            } catch (IllegalArgumentException e) {
                throw new InvalidEventException("payload." + field + " is not a UUID");
            }
        }
    }

    public static Envelope parse(ObjectMapper mapper, String json) {
        JsonNode n;
        try {
            n = mapper.readTree(json);
        } catch (Exception e) {
            throw new InvalidEventException("malformed JSON");
        }
        if (n == null || !n.isObject()) {
            throw new InvalidEventException("event is not a JSON object");
        }
        if (!n.hasNonNull("schemaVersion") || n.get("schemaVersion").asInt(-1) != Outbox.SCHEMA_VERSION) {
            throw new InvalidEventException("unsupported schemaVersion");
        }
        try {
            UUID eventId = UUID.fromString(text(n, "eventId"));
            UUID aggregateId = UUID.fromString(text(n, "aggregateId"));
            String type = text(n, "eventType");
            JsonNode payload = n.get("payload");
            if (payload == null || !payload.isObject()) {
                throw new InvalidEventException("payload is required");
            }
            return new Envelope(eventId, type, aggregateId, n.hasNonNull("correlationId") ? n.get("correlationId").asText() : null, payload);
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("invalid identifier in event");
        }
    }

    private static String text(JsonNode n, String f) {
        if (!n.hasNonNull(f) || !n.get(f).isTextual()) {
            throw new InvalidEventException(f + " is required");
        }
        return n.get(f).asText();
    }
}
