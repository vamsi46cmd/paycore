package io.paycore;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.paycore.Idempotency.Outcome;
import io.paycore.Idempotency.Stored;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Payment orchestrator (API side). Every state change is a single conditional UPDATE whose WHERE clause names the allowed
 * source state, so concurrent callers race on the database row, not in Java. Async work (provider calls, ledger postings)
 * is requested through the transactional outbox and executed by the worker; endpoints that start async work return 202
 * and never claim a final result before the worker has produced it.
 */
@Service
public class PaymentService {
    public record PaymentView(UUID id, String customerId, long amountMinor, String currency, String status, long authorizedMinor,
                              long capturedMinor, long refundedMinor, String failureCode, String captureStatus,
                              Instant createdAt, Instant updatedAt, long version) {}

    public record RefundView(UUID id, UUID paymentId, long amountMinor, String status, String failureCode, Instant createdAt, Instant completedAt) {}

    public record Result(HttpStatus status, Object body) {}

    public static final Set<String> PAYMENT_SORT = Set.of("createdAt", "amountMinor", "updatedAt");

    private final JdbcTemplate jdbc;
    private final Idempotency idem;
    private final Outbox outbox;
    private final Props props;
    private final Counter created;
    private final Counter refundsRejected;

    public PaymentService(JdbcTemplate jdbc, Idempotency idem, Outbox outbox, Props props, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.idem = idem;
        this.outbox = outbox;
        this.props = props;
        this.created = registry.counter("paycore.payments.created");
        this.refundsRejected = registry.counter("paycore.refunds.rejected");
    }

    private static final String PAYMENT_COLS = "p.id, p.customer_id, p.amount_minor, p.currency, p.status, p.authorized_minor, p.captured_minor, p.refunded_minor, "
            + "p.failure_code, (SELECT c.status FROM captures c WHERE c.payment_id = p.id ORDER BY c.created_at DESC LIMIT 1) AS capture_status, p.created_at, p.updated_at, p.version";

    private static PaymentView map(java.sql.ResultSet rs, int i) throws java.sql.SQLException {
        return new PaymentView(rs.getObject("id", UUID.class), rs.getString("customer_id"), rs.getLong("amount_minor"), rs.getString("currency"),
                rs.getString("status"), rs.getLong("authorized_minor"), rs.getLong("captured_minor"), rs.getLong("refunded_minor"),
                rs.getString("failure_code"), rs.getString("capture_status"), rs.getObject("created_at", java.time.OffsetDateTime.class).toInstant(),
                rs.getObject("updated_at", java.time.OffsetDateTime.class).toInstant(), rs.getLong("version"));
    }

    // ------------------------------------------------------------------ create

    public Stored create(UUID merchant, String key, PaymentController.CreatePayment req) {
        return idem.execute(merchant, "payments.create", key, req, () -> {
            long amount = Money.requireValidAmount(req.amountMinor());
            Money.requireCurrency(req.currency());
            UUID id = UUID.randomUUID();
            try {
                jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, idempotency_key) VALUES (?,?,?,?,?,'CREATED',?)",
                        id, merchant, req.customerId(), amount, req.currency(), key);
            } catch (DuplicateKeyException e) {
                throw ApiException.conflict("idempotency_key_reused", "This Idempotency-Key already created a payment with a different request");
            }
            audit(merchant, "payment.created", "PAYMENT", id);
            outbox.enqueue(props.eventsTopic(), "payment.created", id, Map.of("paymentId", id.toString(), "merchantId", merchant.toString(), "amountMinor", amount, "currency", "USD"));
            created.increment();
            return new Outcome(HttpStatus.CREATED, get(merchant, id).orElseThrow());
        });
    }

    // ------------------------------------------------------------------ authorize / cancel

    @Transactional
    public Result authorize(UUID merchant, UUID paymentId) {
        int n = jdbc.update("UPDATE payments SET status = 'PROCESSING', version = version + 1, updated_at = now() WHERE id = ? AND merchant_id = ? AND status = 'CREATED'", paymentId, merchant);
        if (n == 1) {
            audit(merchant, "payment.authorize.requested", "PAYMENT", paymentId);
            outbox.enqueue(props.commandsTopic(), "payment.authorize.requested", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString()));
            return new Result(HttpStatus.ACCEPTED, get(merchant, paymentId).orElseThrow());
        }
        PaymentView v = get(merchant, paymentId).orElseThrow(() -> ApiException.notFound("payment"));
        return switch (v.status()) {
            case "PROCESSING" -> new Result(HttpStatus.ACCEPTED, v);       // repeat of an in-flight request
            case "AUTHORIZED" -> new Result(HttpStatus.OK, v);              // already done
            default -> throw ApiException.conflict("invalid_state", "Cannot authorize a payment in state " + v.status());
        };
    }

    @Transactional
    public PaymentView cancel(UUID merchant, UUID paymentId) {
        int n = jdbc.update("UPDATE payments SET status = 'CANCELED', version = version + 1, updated_at = now() WHERE id = ? AND merchant_id = ? AND status = 'AUTHORIZED' "
                + "AND NOT EXISTS (SELECT 1 FROM captures c WHERE c.payment_id = payments.id AND c.status IN ('PENDING','SUCCEEDED'))", paymentId, merchant);
        if (n == 0) {
            PaymentView v = get(merchant, paymentId).orElseThrow(() -> ApiException.notFound("payment"));
            if ("CANCELED".equals(v.status())) {
                return v;
            }
            throw ApiException.conflict("invalid_state", "Cannot cancel a payment in state " + v.status() + (v.captureStatus() != null ? " with capture " + v.captureStatus() : ""));
        }
        jdbc.update("UPDATE authorizations SET status = 'VOIDED' WHERE payment_id = ?", paymentId);
        audit(merchant, "payment.canceled", "PAYMENT", paymentId);
        outbox.enqueue(props.eventsTopic(), "payment.canceled", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString()));
        return get(merchant, paymentId).orElseThrow();
    }

    // ------------------------------------------------------------------ capture

    public Stored capture(UUID merchant, UUID paymentId, String key, PaymentController.CaptureRequest req) {
        Map<String, Object> fingerprint = new LinkedHashMap<>();
        fingerprint.put("paymentId", paymentId.toString());
        fingerprint.put("amountMinor", req == null ? null : req.amountMinor());
        return idem.execute(merchant, "payments.capture", key, fingerprint, () -> {
            Map<String, Object> p = jdbc.query("SELECT status, authorized_minor FROM payments WHERE id = ? AND merchant_id = ? FOR UPDATE",
                    rs -> rs.next() ? Map.<String, Object>of("status", rs.getString(1), "authorized", rs.getLong(2)) : null, paymentId, merchant);
            if (p == null) {
                throw ApiException.notFound("payment");
            }
            if (!"AUTHORIZED".equals(p.get("status"))) {
                throw ApiException.conflict("invalid_state", "Cannot capture a payment in state " + p.get("status"));
            }
            long authorized = (Long) p.get("authorized");
            long amount = req == null || req.amountMinor() == null ? authorized : Money.requireValidAmount(req.amountMinor());
            if (amount > authorized) {
                throw ApiException.unprocessable("amount_exceeds_authorization", "Capture amount exceeds the authorized amount of " + authorized);
            }
            UUID captureId = UUID.randomUUID();
            try {
                jdbc.update("INSERT INTO captures (id, payment_id, amount_minor, status, idempotency_key) VALUES (?,?,?,'PENDING',?)", captureId, paymentId, amount, key);
            } catch (DuplicateKeyException e) {
                throw ApiException.conflict("capture_exists", "A capture is already pending or completed for this payment");   // one live capture per payment
            }
            audit(merchant, "payment.capture.requested", "CAPTURE", captureId);
            outbox.enqueue(props.commandsTopic(), "payment.capture.requested", paymentId,
                    Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(), "captureId", captureId.toString()));
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("paymentId", paymentId);
            body.put("captureId", captureId);
            body.put("amountMinor", amount);
            body.put("status", "PENDING");
            return new Outcome(HttpStatus.ACCEPTED, body);
        });
    }

    // ------------------------------------------------------------------ refunds

    public Stored refund(UUID merchant, UUID paymentId, String key, PaymentController.RefundRequest req) {
        Map<String, Object> fingerprint = Map.of("paymentId", paymentId.toString(), "amountMinor", req.amountMinor());
        return idem.execute(merchant, "payments.refund", key, fingerprint, () -> {
            long amount = Money.requireValidAmount(req.amountMinor());
            // Reserve the amount atomically. refunded_minor <= captured_minor is also a CHECK constraint, so concurrent refunds can never
            // collectively exceed what was captured; this UPDATE is the serialization point (row lock) between them.
            int n = jdbc.update("UPDATE payments SET refunded_minor = refunded_minor + ?, version = version + 1, updated_at = now() "
                    + "WHERE id = ? AND merchant_id = ? AND status = 'CAPTURED' AND refunded_minor + ? <= captured_minor", amount, paymentId, merchant, amount);
            if (n == 0) {
                refundsRejected.increment();
                PaymentView v = get(merchant, paymentId).orElseThrow(() -> ApiException.notFound("payment"));
                if (!"CAPTURED".equals(v.status())) {
                    throw ApiException.conflict("payment_not_captured", "Only captured payments can be refunded (state: " + v.status() + ")");
                }
                throw ApiException.unprocessable("refund_exceeds_remaining", "Refund exceeds the remaining refundable amount of " + (v.capturedMinor() - v.refundedMinor()));
            }
            UUID refundId = UUID.randomUUID();
            jdbc.update("INSERT INTO refunds (id, payment_id, amount_minor, status, idempotency_key) VALUES (?,?,?,'PENDING',?)", refundId, paymentId, amount, key);
            audit(merchant, "payment.refund.requested", "REFUND", refundId);
            outbox.enqueue(props.commandsTopic(), "payment.refund.requested", paymentId,
                    Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(), "refundId", refundId.toString()));
            return new Outcome(HttpStatus.ACCEPTED, refundView(refundId));
        });
    }

    // ------------------------------------------------------------------ reads (always merchant-scoped)

    @Transactional(readOnly = true)
    public Optional<PaymentView> get(UUID merchant, UUID paymentId) {
        return jdbc.query("SELECT " + PAYMENT_COLS + " FROM payments p WHERE p.id = ? AND p.merchant_id = ?", PaymentService::map, paymentId, merchant).stream().findFirst();
    }

    @Transactional(readOnly = true)
    public Map<String, Object> list(UUID merchant, String status, String customerId, Instant from, Instant to, int page, int size, String sort) {
        if (page < 0 || size < 1 || size > 100) {
            throw ApiException.badRequest("page must be >= 0 and size between 1 and 100");
        }
        if (status != null) {
            try {
                PaymentStatus.valueOf(status);
            } catch (IllegalArgumentException e) {
                throw ApiException.badRequest("unknown status");
            }
        }
        String field = "createdAt";
        String dir = "DESC";
        if (sort != null && !sort.isBlank()) {
            String[] parts = sort.split(",");
            field = parts[0];
            if (!PAYMENT_SORT.contains(field)) {
                throw ApiException.badRequest("sort must be one of " + PAYMENT_SORT);
            }
            if (parts.length > 1) {
                dir = "asc".equalsIgnoreCase(parts[1]) ? "ASC" : "desc".equalsIgnoreCase(parts[1]) ? "DESC" : null;
                if (dir == null) {
                    throw ApiException.badRequest("sort direction must be asc or desc");
                }
            }
        }
        String column = switch (field) { case "amountMinor" -> "p.amount_minor"; case "updatedAt" -> "p.updated_at"; default -> "p.created_at"; };
        StringBuilder where = new StringBuilder(" WHERE p.merchant_id = ?");
        List<Object> args = new ArrayList<>(List.of(merchant));
        if (status != null) { where.append(" AND p.status = ?"); args.add(status); }
        if (customerId != null) { where.append(" AND p.customer_id = ?"); args.add(customerId); }
        if (from != null) { where.append(" AND p.created_at >= ?"); args.add(Timestamp.from(from)); }
        if (to != null) { where.append(" AND p.created_at < ?"); args.add(Timestamp.from(to)); }
        Long total = jdbc.queryForObject("SELECT count(*) FROM payments p" + where, Long.class, args.toArray());
        List<Object> pageArgs = new ArrayList<>(args);
        pageArgs.add(size);
        pageArgs.add(page * size);
        List<PaymentView> items = jdbc.query("SELECT " + PAYMENT_COLS + " FROM payments p" + where + " ORDER BY " + column + " " + dir + ", p.id LIMIT ? OFFSET ?", PaymentService::map, pageArgs.toArray());
        return Map.of("items", items, "page", page, "size", size, "totalElements", total == null ? 0 : total);
    }

    private RefundView refundView(UUID refundId) {
        return jdbc.queryForObject("SELECT id, payment_id, amount_minor, status, failure_code, created_at, completed_at FROM refunds WHERE id = ?", PaymentService::mapRefund, refundId);
    }

    private static RefundView mapRefund(java.sql.ResultSet rs, int i) throws java.sql.SQLException {
        var done = rs.getObject("completed_at", java.time.OffsetDateTime.class);
        return new RefundView(rs.getObject("id", UUID.class), rs.getObject("payment_id", UUID.class), rs.getLong("amount_minor"), rs.getString("status"),
                rs.getString("failure_code"), rs.getObject("created_at", java.time.OffsetDateTime.class).toInstant(), done == null ? null : done.toInstant());
    }

    @Transactional(readOnly = true)
    public List<RefundView> refunds(UUID merchant, UUID paymentId) {
        get(merchant, paymentId).orElseThrow(() -> ApiException.notFound("payment"));
        return jdbc.query("SELECT id, payment_id, amount_minor, status, failure_code, created_at, completed_at FROM refunds WHERE payment_id = ? ORDER BY created_at, id LIMIT 500", PaymentService::mapRefund, paymentId);
    }

    @Transactional(readOnly = true)
    public Map<String, Object> transactions(UUID merchant, String type, int page, int size) {
        if (page < 0 || size < 1 || size > 100) {
            throw ApiException.badRequest("page must be >= 0 and size between 1 and 100");
        }
        if (type != null && !Set.of("CAPTURE", "REFUND").contains(type)) {
            throw ApiException.badRequest("type must be CAPTURE or REFUND");
        }
        String union = "SELECT 'CAPTURE' AS type, c.id, c.payment_id, c.amount_minor, c.status, c.created_at FROM captures c JOIN payments p ON p.id = c.payment_id WHERE p.merchant_id = ? "
                + "UNION ALL SELECT 'REFUND', r.id, r.payment_id, r.amount_minor, r.status, r.created_at FROM refunds r JOIN payments p ON p.id = r.payment_id WHERE p.merchant_id = ?";
        String sql = "SELECT * FROM (" + union + ") t" + (type == null ? "" : " WHERE t.type = '" + type + "'") + " ORDER BY created_at DESC, id LIMIT ? OFFSET ?";
        List<Map<String, Object>> rows = Camel.list(jdbc.queryForList(sql, merchant, merchant, size, page * size));
        return Map.of("items", rows, "page", page, "size", size);
    }

    private void audit(UUID merchant, String action, String entityType, UUID entityId) {
        jdbc.update("INSERT INTO audit_log (id, merchant_id, actor, action, entity_type, entity_id, request_id) VALUES (?,?,?,?,?,?,?)",
                UUID.randomUUID(), merchant, "merchant", action, entityType, entityId, MDC.get("requestId"));
    }
}
