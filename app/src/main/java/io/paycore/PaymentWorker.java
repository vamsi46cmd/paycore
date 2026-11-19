package io.paycore;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.paycore.Events.Envelope;
import io.paycore.Events.InvalidEventException;
import io.paycore.Events.TransientProcessingException;
import io.paycore.Provider.Outcome;
import io.paycore.Provider.ProviderTimeoutException;
import io.paycore.Provider.ProviderUnavailableException;
import io.paycore.Provider.Request;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Payment command consumer. Delivery from Kafka is at-least-once, so every handler is idempotent at THREE levels:
 *  1. a processed_events marker is inserted in the same transaction as the effects (duplicate events become no-ops);
 *  2. state changes are conditional UPDATEs ("... WHERE status = 'PROCESSING'") so a stale or replayed command changes nothing;
 *  3. the ledger rejects a second journal for the same capture/refund (UNIQUE reference).
 * Provider calls happen OUTSIDE database transactions, with stable operation ids. After a timeout the worker first LOOKS UP what the
 * provider recorded; it only retries when nothing was recorded, and a retry reuses the same operation id (the provider is
 * idempotent by id), so an ambiguous outcome can never turn into a second capture or refund.
 */
@Component
@ConditionalOnProperty(name = "paycore.roles.worker", havingValue = "true")
public class PaymentWorker {
    private static final Logger log = LoggerFactory.getLogger(PaymentWorker.class);
    static final String CONSUMER = "payment-worker";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final Provider provider;
    private final Ledger ledger;
    private final Outbox outbox;
    private final Props props;
    private final MeterRegistry metrics;
    private final TransactionTemplate tx;
    private final TransactionTemplate txNew;

    public PaymentWorker(JdbcTemplate jdbc, ObjectMapper mapper, Provider provider, Ledger ledger, Outbox outbox, Props props,
                         MeterRegistry metrics, PlatformTransactionManager tm) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.provider = provider;
        this.ledger = ledger;
        this.outbox = outbox;
        this.props = props;
        this.metrics = metrics;
        this.tx = new TransactionTemplate(tm);
        this.txNew = new TransactionTemplate(tm);
        this.txNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @KafkaListener(topics = "${paycore.topics.prefix:}payments.commands.v1", groupId = "${paycore.consumer-group-prefix:paycore}-payment-worker",
            concurrency = "${paycore.worker.concurrency:3}")
    public void onCommand(ConsumerRecord<String, String> rec) {
        Envelope e = Events.parse(mapper, rec.value());
        if (e.correlationId() != null) {
            MDC.put("requestId", e.correlationId());
        }
        MDC.put("paymentId", e.aggregateId().toString());
        Timer.Sample sample = Timer.start(metrics);
        try {
            handle(e);
        } finally {
            sample.stop(metrics.timer("paycore.worker.processing", "type", e.eventType()));
            MDC.remove("requestId");
            MDC.remove("paymentId");
        }
    }

    /** Package-visible so tests can deliver (and redeliver) an event without Kafka. */
    void handle(Envelope e) {
        switch (e.eventType()) {
            case "payment.authorize.requested" -> authorize(e);
            case "payment.capture.requested" -> capture(e);
            case "payment.refund.requested" -> refund(e);
            default -> throw new InvalidEventException("unknown command type " + e.eventType());
        }
    }

    // ------------------------------------------------------------------ authorize

    private void authorize(Envelope e) {
        UUID paymentId = e.uuid("paymentId");
        Map<String, Object> p = payment(paymentId);
        if (!"PROCESSING".equals(p.get("status"))) {
            markProcessedOnly(e);   // duplicate or stale command: nothing to do
            return;
        }
        String op = "auth-" + paymentId;
        long amount = (Long) p.get("amount_minor");
        String scenario = (String) p.get("customer_id");
        UUID attempt = startAttempt(paymentId, "AUTHORIZE", op);
        Outcome o = callProvider(new Request(op, "AUTHORIZE", paymentId, amount, scenario), attempt);
        applyAuthorize(e.eventId(), paymentId, amount, o, attempt);
        if ("cust_duplicate_callback".equals(scenario)) {
            applyAuthorize(UUID.randomUUID(), paymentId, amount, o, attempt);   // duplicate provider callback: must be a no-op
        }
    }

    void applyAuthorize(UUID eventId, UUID paymentId, long amount, Outcome o, UUID attempt) {
        tx.executeWithoutResult(s -> {
            if (!markProcessed(eventId)) {
                return;
            }
            UUID merchant = jdbc.queryForObject("SELECT merchant_id FROM payments WHERE id = ?", UUID.class, paymentId);
            if (o.succeeded()) {
                int n = jdbc.update("UPDATE payments SET status = 'AUTHORIZED', authorized_minor = amount_minor, version = version + 1, updated_at = now() WHERE id = ? AND status = 'PROCESSING'", paymentId);
                if (n == 1) {
                    jdbc.update("INSERT INTO authorizations (id, payment_id, authorized_minor, status, provider_reference) VALUES (?,?,?, 'AUTHORIZED', ?) ON CONFLICT DO NOTHING", UUID.randomUUID(), paymentId, amount, o.operationId());
                    outbox.enqueue(props.eventsTopic(), "payment.authorized", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(), "amountMinor", amount));
                    metrics.counter("paycore.payments.authorized").increment();
                }
            } else {
                int n = jdbc.update("UPDATE payments SET status = 'FAILED', failure_code = ?, version = version + 1, updated_at = now() WHERE id = ? AND status = 'PROCESSING'", o.errorCode(), paymentId);
                if (n == 1) {
                    outbox.enqueue(props.eventsTopic(), "payment.failed", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(), "failureCode", String.valueOf(o.errorCode())));
                    metrics.counter("paycore.payments.failed").increment();
                }
            }
            finishAttempt(attempt, o.succeeded() ? "SUCCEEDED" : "FAILED", o.errorCode());
        });
    }

    // ------------------------------------------------------------------ capture

    private void capture(Envelope e) {
        UUID paymentId = e.uuid("paymentId");
        UUID captureId = e.uuid("captureId");
        Map<String, Object> c = jdbc.query("SELECT status, amount_minor FROM captures WHERE id = ?",
                rs -> rs.next() ? Map.<String, Object>of("status", rs.getString(1), "amount", rs.getLong(2)) : null, captureId);
        if (c == null || !"PENDING".equals(c.get("status"))) {
            markProcessedOnly(e);
            return;
        }
        long amount = (Long) c.get("amount");
        String op = "cap-" + captureId;
        String scenario = (String) payment(paymentId).get("customer_id");
        UUID attempt = startAttempt(paymentId, "CAPTURE", op);
        Outcome o = callProvider(new Request(op, "CAPTURE", paymentId, amount, scenario), attempt);
        applyCapture(e.eventId(), paymentId, captureId, amount, o, attempt);
        if ("cust_duplicate_callback".equals(scenario)) {
            applyCapture(UUID.randomUUID(), paymentId, captureId, amount, o, attempt);
        }
    }

    void applyCapture(UUID eventId, UUID paymentId, UUID captureId, long amount, Outcome o, UUID attempt) {
        tx.executeWithoutResult(s -> {
            if (!markProcessed(eventId)) {
                return;
            }
            UUID merchant = jdbc.queryForObject("SELECT merchant_id FROM payments WHERE id = ?", UUID.class, paymentId);
            if (o.succeeded()) {
                int n = jdbc.update("UPDATE captures SET status = 'SUCCEEDED', provider_reference = ?, completed_at = now() WHERE id = ? AND status = 'PENDING'", o.operationId(), captureId);
                if (n == 0) {
                    return;   // another delivery already completed this capture
                }
                int p = jdbc.update("UPDATE payments SET status = 'CAPTURED', captured_minor = ?, version = version + 1, updated_at = now() WHERE id = ? AND status = 'AUTHORIZED'", amount, paymentId);
                if (p != 1) {
                    throw new IllegalStateException("capture succeeded at the provider but payment " + paymentId + " is not AUTHORIZED; manual review required");
                }
                UUID journal = ledger.postCapture(merchant, captureId, amount);
                outbox.enqueue(props.eventsTopic(), "payment.captured", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(),
                        "captureId", captureId.toString(), "amountMinor", amount, "journalId", journal.toString()));
                metrics.counter("paycore.payments.captured").increment();
            } else {
                int n = jdbc.update("UPDATE captures SET status = 'FAILED', failure_code = ?, completed_at = now() WHERE id = ? AND status = 'PENDING'", o.errorCode(), captureId);
                if (n == 1) {
                    outbox.enqueue(props.eventsTopic(), "payment.capture_failed", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(),
                            "captureId", captureId.toString(), "failureCode", String.valueOf(o.errorCode())));
                }
            }
            finishAttempt(attempt, o.succeeded() ? "SUCCEEDED" : "FAILED", o.errorCode());
        });
    }

    // ------------------------------------------------------------------ refund

    private void refund(Envelope e) {
        UUID paymentId = e.uuid("paymentId");
        UUID refundId = e.uuid("refundId");
        Map<String, Object> r = jdbc.query("SELECT status, amount_minor FROM refunds WHERE id = ?",
                rs -> rs.next() ? Map.<String, Object>of("status", rs.getString(1), "amount", rs.getLong(2)) : null, refundId);
        if (r == null || !"PENDING".equals(r.get("status"))) {
            markProcessedOnly(e);
            return;
        }
        long amount = (Long) r.get("amount");
        String op = "ref-" + refundId;
        String scenario = (String) payment(paymentId).get("customer_id");
        UUID attempt = startAttempt(paymentId, "REFUND", op);
        Outcome o = callProvider(new Request(op, "REFUND", paymentId, amount, scenario), attempt);
        applyRefund(e.eventId(), paymentId, refundId, amount, o, attempt);
        if ("cust_duplicate_callback".equals(scenario)) {
            applyRefund(UUID.randomUUID(), paymentId, refundId, amount, o, attempt);
        }
    }

    void applyRefund(UUID eventId, UUID paymentId, UUID refundId, long amount, Outcome o, UUID attempt) {
        tx.executeWithoutResult(s -> {
            if (!markProcessed(eventId)) {
                return;
            }
            UUID merchant = jdbc.queryForObject("SELECT merchant_id FROM payments WHERE id = ?", UUID.class, paymentId);
            if (o.succeeded()) {
                int n = jdbc.update("UPDATE refunds SET status = 'SUCCEEDED', provider_reference = ?, completed_at = now() WHERE id = ? AND status = 'PENDING'", o.operationId(), refundId);
                if (n == 0) {
                    return;
                }
                UUID journal = ledger.postRefund(merchant, refundId, amount);
                outbox.enqueue(props.eventsTopic(), "refund.completed", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(),
                        "refundId", refundId.toString(), "amountMinor", amount, "journalId", journal.toString()));
                metrics.counter("paycore.refunds.completed").increment();
            } else {
                int n = jdbc.update("UPDATE refunds SET status = 'FAILED', failure_code = ?, completed_at = now() WHERE id = ? AND status = 'PENDING'", o.errorCode(), refundId);
                if (n == 1) {
                    // A failed refund must not stay reserved: release the amount so it can be refunded again.
                    jdbc.update("UPDATE payments SET refunded_minor = refunded_minor - ?, version = version + 1, updated_at = now() WHERE id = ?", amount, paymentId);
                    outbox.enqueue(props.eventsTopic(), "refund.failed", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(),
                            "refundId", refundId.toString(), "failureCode", String.valueOf(o.errorCode())));
                    metrics.counter("paycore.refunds.failed").increment();
                }
            }
            finishAttempt(attempt, o.succeeded() ? "SUCCEEDED" : "FAILED", o.errorCode());
        });
    }

    // ------------------------------------------------------------------ shared

    /** Calls the provider outside any DB transaction; resolves ambiguous outcomes by lookup before any retry. */
    private Outcome callProvider(Request req, UUID attempt) {
        try {
            return provider.execute(req);
        } catch (ProviderTimeoutException t) {
            Optional<Outcome> recorded = provider.lookup(req.operationId());
            if (recorded.isPresent()) {
                log.info("Provider timed out but had recorded {} for {}; using the recorded outcome (no retry)", recorded.get().result(), req.operationId());
                return recorded.get();
            }
            endAttemptRetryable(attempt, "timeout_unknown");
            throw new TransientProcessingException("provider timed out and recorded nothing for " + req.operationId() + "; will retry with the same operation id");
        } catch (ProviderUnavailableException u) {
            endAttemptRetryable(attempt, "provider_unavailable");
            throw new TransientProcessingException("provider unavailable for " + req.operationId());
        }
    }

    private Map<String, Object> payment(UUID paymentId) {
        return jdbc.queryForMap("SELECT status, amount_minor, customer_id FROM payments WHERE id = ?", paymentId);
    }

    private boolean markProcessed(UUID eventId) {
        return jdbc.update("INSERT INTO processed_events (consumer, event_id) VALUES (?, ?) ON CONFLICT DO NOTHING", CONSUMER, eventId) == 1;
    }

    private void markProcessedOnly(Envelope e) {
        tx.executeWithoutResult(s -> markProcessed(e.eventId()));
    }

    private UUID startAttempt(UUID paymentId, String operation, String providerRef) {
        return txNew.execute(s -> {
            UUID id = UUID.randomUUID();
            jdbc.update("INSERT INTO payment_attempts (id, payment_id, operation, attempt_number, provider_reference, status) "
                    + "VALUES (?, ?, ?, (SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM payment_attempts WHERE payment_id = ? AND operation = ?), ?, 'STARTED')",
                    id, paymentId, operation, paymentId, operation, providerRef);
            return id;
        });
    }

    private void endAttemptRetryable(UUID attempt, String code) {
        txNew.executeWithoutResult(s -> jdbc.update("UPDATE payment_attempts SET status = 'RETRYABLE_ERROR', error_code = ?, completed_at = now() WHERE id = ?", code, attempt));
    }

    private void finishAttempt(UUID attempt, String status, String code) {
        jdbc.update("UPDATE payment_attempts SET status = ?, error_code = ?, completed_at = now() WHERE id = ?", status, code, attempt);
    }
}
