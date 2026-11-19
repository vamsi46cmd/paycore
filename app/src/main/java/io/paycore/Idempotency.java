package io.paycore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Database-backed idempotency, scoped by (merchant, operation, key).
 *
 * The key row is inserted in the SAME transaction as the business effect and then updated with the response, so:
 *  - success commits effect + key + response atomically (no window where the effect exists but a retry re-executes it);
 *  - any failure or crash rolls everything back, leaving no key, so a retry re-executes cleanly;
 *  - two simultaneous requests with one key: the second INSERT blocks on the first's uncommitted row, then sees the
 *    conflict, compares fingerprints and replays the stored response (or gets 409 if the payload differs).
 * Only successful (2xx) outcomes are stored; deterministic errors are re-evaluated on retry (documented choice).
 * Keys expire after paycore.idempotency.ttl; a purge job deletes expired rows, after which a key can be reused.
 */
@Service
public class Idempotency {
    public record Outcome(HttpStatus status, Object body) {}

    public record Stored(int status, String body, boolean replayed) {}

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final Props props;
    private final Counter replays;
    private final Counter conflicts;

    public Idempotency(JdbcTemplate jdbc, ObjectMapper mapper, Props props, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.props = props;
        this.replays = registry.counter("paycore.idempotency.duplicates.prevented");
        this.conflicts = registry.counter("paycore.idempotency.conflicts");
    }

    @Transactional
    public Stored execute(UUID merchantId, String operation, String key, Object request, Supplier<Outcome> action) {
        if (key == null || key.isBlank() || key.length() > 100) {
            throw ApiException.badRequest("Idempotency-Key must be 1-100 characters");
        }
        String fingerprint = SecurityConfig.sha256(canonical(mapper.valueToTree(request)));
        Timestamp expires = Timestamp.from(Instant.now().plus(props.idempotency().ttl()));
        if (claim(merchantId, operation, key, fingerprint, expires)) {
            Outcome o = action.get();
            String body = write(o.body());
            jdbc.update("UPDATE idempotency_keys SET response_status = ?, response_body = ? WHERE merchant_id = ? AND operation = ? AND idem_key = ?",
                    o.status().value(), body, merchantId, operation, key);
            return new Stored(o.status().value(), body, false);
        }
        Map<String, Object> row = jdbc.queryForMap("SELECT fingerprint, response_status, response_body FROM idempotency_keys WHERE merchant_id = ? AND operation = ? AND idem_key = ?",
                merchantId, operation, key);
        if (!fingerprint.equals(row.get("fingerprint"))) {
            conflicts.increment();
            throw ApiException.conflict("idempotency_key_conflict", "Idempotency-Key was already used with a different request payload");
        }
        replays.increment();
        return new Stored((Integer) row.get("response_status"), (String) row.get("response_body"), true);
    }

    /** @return true if this request owns the key; false if a committed row already exists. Expired rows are replaced. */
    private boolean claim(UUID merchantId, String operation, String key, String fingerprint, Timestamp expires) {
        for (int i = 0; i < 2; i++) {
            int n = jdbc.update("INSERT INTO idempotency_keys (merchant_id, operation, idem_key, fingerprint, response_status, response_body, expires_at) "
                    + "VALUES (?,?,?,?,0,'',?) ON CONFLICT DO NOTHING", merchantId, operation, key, fingerprint, expires);
            if (n == 1) {
                return true;
            }
            int purged = jdbc.update("DELETE FROM idempotency_keys WHERE merchant_id = ? AND operation = ? AND idem_key = ? AND expires_at < now()", merchantId, operation, key);
            if (purged == 0) {
                return false;
            }
        }
        return false;
    }

    /** Retention: remove expired keys. */
    @org.springframework.scheduling.annotation.Scheduled(fixedDelayString = "PT10M", initialDelayString = "PT1M")
    public void purgeExpired() {
        jdbc.update("DELETE FROM idempotency_keys WHERE expires_at < now()");
    }

    private String write(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Canonical JSON: object keys sorted recursively, so logically equal payloads produce equal fingerprints. */
    static String canonical(JsonNode n) {
        if (n.isObject()) {
            TreeMap<String, JsonNode> sorted = new TreeMap<>();
            Iterator<Map.Entry<String, JsonNode>> it = n.fields();
            while (it.hasNext()) {
                var e = it.next();
                sorted.put(e.getKey(), e.getValue());
            }
            StringBuilder sb = new StringBuilder("{");
            sorted.forEach((k, v) -> sb.append(sb.length() > 1 ? "," : "").append('"').append(k).append("\":").append(canonical(v)));
            return sb.append('}').toString();
        }
        if (n.isArray()) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < n.size(); i++) {
                sb.append(i > 0 ? "," : "").append(canonical(n.get(i)));
            }
            return sb.append(']').toString();
        }
        return n.toString();
    }
}
