package io.paycore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Transactional outbox, write side. enqueue() REQUIRES an active transaction (MANDATORY), so an event row can only be
 * created atomically with the business change that caused it: either both commit or neither does.
 * Envelope (schema v1): {eventId, eventType, schemaVersion, aggregateId, occurredAt, correlationId, payload}.
 */
@Service
public class Outbox {
    public static final int SCHEMA_VERSION = 1;
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public Outbox(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID enqueue(String topic, String eventType, UUID aggregateId, Map<String, Object> payload) {
        UUID eventId = UUID.randomUUID();
        ObjectNode env = mapper.createObjectNode();
        env.put("eventId", eventId.toString());
        env.put("eventType", eventType);
        env.put("schemaVersion", SCHEMA_VERSION);
        env.put("aggregateId", aggregateId.toString());
        env.put("occurredAt", Instant.now().toString());
        String corr = MDC.get("requestId");
        if (corr != null) {
            env.put("correlationId", corr);
        }
        env.set("payload", mapper.valueToTree(payload));
        jdbc.update("INSERT INTO outbox_events (id, aggregate_id, topic, event_type, payload) VALUES (?,?,?,?,?::jsonb)",
                eventId, aggregateId, topic, eventType, env.toString());
        return eventId;
    }
}
