package io.paycore;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.paycore.Events.Envelope;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Webhook worker: (1) consumes payment events and fans them out into webhook_deliveries (UNIQUE (event_id, endpoint_id) makes
 * redelivered events harmless); (2) delivers due rows with HMAC signatures. Delivery is AT-LEAST-ONCE: if the receiver
 * processed a request but the response was lost, the retry sends the same event again with the same PayCore-Event-Id.
 * Retries use exponential backoff (base * 2^(attempt-1), capped at 5 minutes) up to max-attempts, then the delivery is DEAD
 * and can be replayed through the API.
 */
@Component
@ConditionalOnProperty(name = "paycore.roles.webhook", havingValue = "true")
public class WebhookWorker {
    private static final Logger log = LoggerFactory.getLogger(WebhookWorker.class);
    static final String CONSUMER = "webhook-fanout";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final Props props;
    private final UrlPolicy policy;
    private final MeterRegistry metrics;
    private final HttpClient http;
    private final TransactionTemplate tx;

    public WebhookWorker(JdbcTemplate jdbc, ObjectMapper mapper, Props props, UrlPolicy policy, MeterRegistry metrics, PlatformTransactionManager tm) {
        this.tx = new TransactionTemplate(tm);
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.props = props;
        this.policy = policy;
        this.metrics = metrics;
        // Redirects are never followed: a redirect could point at an internal address.
        this.http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).connectTimeout(props.webhooks().timeout()).build();
    }

    @KafkaListener(topics = "${paycore.topics.prefix:}payments.events.v1", groupId = "${paycore.consumer-group-prefix:paycore}-webhook-worker", concurrency = "2")
    public void onEvent(ConsumerRecord<String, String> rec) {
        fanOut(Events.parse(mapper, rec.value()), rec.value());
    }

    /** Explicit transaction: the processed marker and the delivery rows commit together, so a crash cannot lose a fan-out. */
    public void fanOut(Envelope e, String rawEnvelope) {
        tx.executeWithoutResult(s -> fanOutInTransaction(e, rawEnvelope));
    }

    private void fanOutInTransaction(Envelope e, String rawEnvelope) {
        if (jdbc.update("INSERT INTO processed_events (consumer, event_id) VALUES (?, ?) ON CONFLICT DO NOTHING", CONSUMER, e.eventId()) == 0) {
            return;   // redelivered event
        }
        if (!e.payload().hasNonNull("merchantId")) {
            return;
        }
        UUID merchant = UUID.fromString(e.payload().get("merchantId").asText());
        List<UUID> endpoints = jdbc.queryForList("SELECT id FROM webhook_endpoints WHERE merchant_id = ? AND active AND (? = ANY (event_types) OR '*' = ANY (event_types))",
                UUID.class, merchant, e.eventType());
        for (UUID endpoint : endpoints) {
            jdbc.update("INSERT INTO webhook_deliveries (id, event_id, endpoint_id, merchant_id, event_type, payload) VALUES (?,?,?,?,?,?::jsonb) ON CONFLICT (event_id, endpoint_id) DO NOTHING",
                    UUID.randomUUID(), e.eventId(), endpoint, merchant, e.eventType(), rawEnvelope);
        }
    }

    @Scheduled(fixedDelay = 500)
    public void tick() {
        try {
            deliverDue();
        } catch (Exception ex) {
            log.warn("Webhook delivery batch failed: {}", ex.toString());
        }
    }

    private record Due(UUID id, UUID eventId, UUID endpointId, String url, String secret, String payload, int attempts) {}

    /** Locks due rows (SKIP LOCKED: several webhook workers never deliver the same row at once), sends, records the result. Explicit transaction: the locks live until it ends. */
    public int deliverDue() {
        Integer n = tx.execute(s -> deliverDueInTransaction());
        return n == null ? 0 : n;
    }

    private int deliverDueInTransaction() {
        List<Due> due = jdbc.query("SELECT d.id, d.event_id, d.endpoint_id, e.url, e.secret, d.payload::text AS payload, d.attempt_count FROM webhook_deliveries d "
                + "JOIN webhook_endpoints e ON e.id = d.endpoint_id WHERE d.status = 'PENDING' AND d.next_attempt_at <= now() ORDER BY d.next_attempt_at LIMIT ? FOR UPDATE OF d SKIP LOCKED",
                (rs, i) -> new Due(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class), rs.getObject(3, UUID.class), rs.getString(4), rs.getString(5), rs.getString(6), rs.getInt(7)),
                props.webhooks().batchSize());
        for (Due d : due) {
            deliver(d);
        }
        return due.size();
    }

    private void deliver(Due d) {
        int attemptNo = d.attempts() + 1;
        long start = System.nanoTime();
        Integer status = null;
        String error = null;
        try {
            var uri = policy.validate(d.url());   // re-checked at delivery time
            long ts = System.currentTimeMillis() / 1000;
            HttpRequest req = HttpRequest.newBuilder(uri).timeout(props.webhooks().timeout())
                    .header("Content-Type", "application/json")
                    .header("PayCore-Event-Id", d.eventId().toString())
                    .header("PayCore-Delivery-Id", d.id().toString())
                    .header("PayCore-Endpoint-Id", d.endpointId().toString())
                    .header("PayCore-Signature", Webhook.header(d.secret(), ts, d.payload()))
                    .POST(HttpRequest.BodyPublishers.ofString(d.payload())).build();
            status = http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (ApiException e) {
            error = "blocked by policy: " + e.getMessage();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            error = "interrupted";
        } catch (Exception e) {
            error = e.getClass().getSimpleName();
        }
        long ms = (System.nanoTime() - start) / 1_000_000;
        boolean ok = status != null && status >= 200 && status < 300;
        metrics.counter("paycore.webhook.attempts").increment();
        jdbc.update("INSERT INTO webhook_attempts (id, delivery_id, attempt_number, response_status, error, duration_ms) VALUES (?,?,?,?,?,?)", UUID.randomUUID(), d.id(), attemptNo, status, error, ms);
        if (ok) {
            jdbc.update("UPDATE webhook_deliveries SET status = 'DELIVERED', attempt_count = ?, delivered_at = now(), last_error = NULL WHERE id = ?", attemptNo, d.id());
            metrics.counter("paycore.webhook.delivered").increment();
            return;
        }
        metrics.counter("paycore.webhook.failures").increment();
        String err = error != null ? error : "HTTP " + status;
        if (attemptNo >= props.webhooks().maxAttempts()) {
            jdbc.update("UPDATE webhook_deliveries SET status = 'DEAD', attempt_count = ?, last_error = ? WHERE id = ?", attemptNo, err, d.id());
        } else {
            long delayMs = Math.min(300_000L, props.webhooks().backoffBase().toMillis() * (1L << (attemptNo - 1)));
            jdbc.update("UPDATE webhook_deliveries SET attempt_count = ?, last_error = ?, next_attempt_at = now() + (? * interval '1 millisecond') WHERE id = ?", attemptNo, err, delayMs, d.id());
        }
    }
}
