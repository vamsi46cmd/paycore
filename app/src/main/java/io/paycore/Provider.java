package io.paycore;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.TransactionDefinition;

/**
 * Deterministic SIMULATED payment provider. It never talks to a real institution and handles no card data.
 *
 * Guarantees (the contract the rest of the system relies on)
 *  1. Operations are idempotent by operationId: executing the same operationId again returns the ORIGINAL recorded outcome and
 *     never produces a second effect. Operation ids are stable (auth-<paymentId>, cap-<captureId>, ref-<refundId>).
 *  2. lookup(operationId) reports what the provider actually recorded, so a caller that timed out can learn the truth
 *     before deciding to retry.
 *  3. The provider's own record is durable and committed independently of the caller's transaction.
 *
 * Scenarios are chosen deterministically by the payment's customerId:
 *   cust_decline                 authorization FAILED (card_declined)
 *   cust_capture_fail            capture FAILED (capture_declined)
 *   cust_refund_fail             refund FAILED (refund_declined)
 *   cust_unavailable             the first N calls to any operation throw ProviderUnavailable (nothing recorded)
 *   cust_timeout_no_effect       first call times out WITHOUT being recorded; lookup says unknown; retry succeeds
 *   cust_timeout_after_success   first call is recorded as SUCCEEDED, then the caller sees a timeout (ambiguous outcome)
 *   cust_delayed                 succeeds after a configured delay
 *   cust_duplicate_callback      succeeds; the worker additionally processes the same result a second time (duplicate callback)
 *   anything else                succeeds
 */
@Component
public class Provider {
    public static class ProviderTimeoutException extends Exception {
        public ProviderTimeoutException(String m) { super(m); }
    }

    public static class ProviderUnavailableException extends Exception {
        public ProviderUnavailableException(String m) { super(m); }
    }

    public record Request(String operationId, String type, UUID paymentId, long amountMinor, String scenario) {}

    public record Outcome(String operationId, String result, String errorCode) {
        public boolean succeeded() { return "SUCCEEDED".equals(result); }
    }

    private final JdbcTemplate jdbc;
    private final TransactionTemplate independentTx;
    private final Props props;

    public Provider(JdbcTemplate jdbc, PlatformTransactionManager tm, Props props) {
        this.jdbc = jdbc;
        this.props = props;
        this.independentTx = new TransactionTemplate(tm);
        this.independentTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public Outcome execute(Request r) throws ProviderTimeoutException, ProviderUnavailableException {
        int call = independentTx.execute(s -> {
            jdbc.update("INSERT INTO provider_call_log (operation_id, calls) VALUES (?, 1) ON CONFLICT (operation_id) DO UPDATE SET calls = provider_call_log.calls + 1", r.operationId());
            return jdbc.queryForObject("SELECT calls FROM provider_call_log WHERE operation_id = ?", Integer.class, r.operationId());
        });
        String scenario = r.scenario() == null ? "" : r.scenario();
        switch (scenario) {
            case "cust_unavailable" -> {
                if (call <= props.provider().unavailableFailures()) {
                    throw new ProviderUnavailableException("provider temporarily unavailable");
                }
            }
            case "cust_timeout_no_effect" -> {
                if (call == 1) {
                    throw new ProviderTimeoutException("provider timed out before recording the operation");
                }
            }
            case "cust_delayed" -> sleep(props.provider().delayMs());
            default -> { }
        }
        Optional<Outcome> existing = lookup(r.operationId());
        Outcome outcome = existing.orElseGet(() -> record(r, decide(r)));
        if ("cust_timeout_after_success".equals(scenario) && call == 1) {
            throw new ProviderTimeoutException("provider recorded the operation but the response was lost");
        }
        return outcome;
    }

    public Optional<Outcome> lookup(String operationId) {
        return jdbc.query("SELECT operation_id, outcome, error_code FROM provider_operations WHERE operation_id = ?",
                (rs, i) -> new Outcome(rs.getString(1), rs.getString(2), rs.getString(3)), operationId).stream().findFirst();
    }

    private Outcome decide(Request r) {
        String s = r.scenario() == null ? "" : r.scenario();
        if ("AUTHORIZE".equals(r.type()) && s.equals("cust_decline")) {
            return new Outcome(r.operationId(), "FAILED", "card_declined");
        }
        if ("CAPTURE".equals(r.type()) && s.equals("cust_capture_fail")) {
            return new Outcome(r.operationId(), "FAILED", "capture_declined");
        }
        if ("REFUND".equals(r.type()) && s.equals("cust_refund_fail")) {
            return new Outcome(r.operationId(), "FAILED", "refund_declined");
        }
        return new Outcome(r.operationId(), "SUCCEEDED", null);
    }

    private Outcome record(Request r, Outcome o) {
        independentTx.executeWithoutResult(s -> jdbc.update("INSERT INTO provider_operations (operation_id, op_type, payment_id, amount_minor, outcome, error_code) VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                r.operationId(), r.type(), r.paymentId(), r.amountMinor(), o.result(), o.errorCode()));
        return lookup(r.operationId()).orElse(o);   // re-read: a concurrent identical request may have recorded first
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
