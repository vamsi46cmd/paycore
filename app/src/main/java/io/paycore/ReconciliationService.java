package io.paycore;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Reconciliation compares four independent records of the same financial truth: payment rows, capture/refund rows, the simulated
 * provider's own record, and the ledger. Every query is a plain read; discrepancies found are stored, never invented or hidden.
 *
 * Repair is deliberately narrow (repair=true): for things STUCK in PROCESSING/PENDING, or whose provider outcome was recorded but never
 * applied, it re-queues the original command through the outbox. That is safe because command handling is idempotent. Reconciliation
 * NEVER edits payment amounts or ledger rows; a real mismatch is reported for a human, and correcting posted money is done with a
 * reversing journal (Ledger.reverse), not by rewriting history.
 */
@Service
public class ReconciliationService {
    private record Check(String type, String sql) {}

    private static final String STUCK = "now() - (? * interval '1 second')";

    private static final List<Check> CHECKS = List.of(
        new Check("MISSING_CAPTURE_JOURNAL", "SELECT p.id AS payment_id, c.id AS reference_id, 'payment is CAPTURED but its capture has no ledger journal' AS details FROM payments p "
            + "JOIN captures c ON c.payment_id = p.id AND c.status = 'SUCCEEDED' LEFT JOIN ledger_journals j ON j.reference_type = 'CAPTURE' AND j.reference_id = c.id WHERE p.status = 'CAPTURED' AND j.id IS NULL"),
        new Check("CAPTURE_LEDGER_AMOUNT_MISMATCH", "SELECT c.payment_id, c.id, 'capture ' || c.amount_minor || ' vs ledger debit ' || x.debit || ' / credit ' || x.credit FROM captures c "
            + "JOIN ledger_journals j ON j.reference_type = 'CAPTURE' AND j.reference_id = c.id JOIN (SELECT journal_id, sum(credit_minor) AS credit, sum(debit_minor) AS debit FROM ledger_entries GROUP BY journal_id) x ON x.journal_id = j.id "
            + "WHERE c.status = 'SUCCEEDED' AND (x.credit <> c.amount_minor OR x.debit <> c.amount_minor)"),
        new Check("MISSING_REFUND_JOURNAL", "SELECT r.payment_id, r.id, 'refund SUCCEEDED but has no ledger journal' FROM refunds r LEFT JOIN ledger_journals j ON j.reference_type = 'REFUND' AND j.reference_id = r.id WHERE r.status = 'SUCCEEDED' AND j.id IS NULL"),
        new Check("REFUND_LEDGER_AMOUNT_MISMATCH", "SELECT r.payment_id, r.id, 'refund ' || r.amount_minor || ' vs ledger debit ' || x.debit || ' / credit ' || x.credit FROM refunds r "
            + "JOIN ledger_journals j ON j.reference_type = 'REFUND' AND j.reference_id = r.id JOIN (SELECT journal_id, sum(credit_minor) AS credit, sum(debit_minor) AS debit FROM ledger_entries GROUP BY journal_id) x ON x.journal_id = j.id "
            + "WHERE r.status = 'SUCCEEDED' AND (x.credit <> r.amount_minor OR x.debit <> r.amount_minor)"),
        new Check("JOURNAL_WITHOUT_SUCCESSFUL_SOURCE", "SELECT NULL::uuid, j.reference_id, 'ledger ' || j.reference_type || ' journal whose source is not SUCCEEDED' FROM ledger_journals j "
            + "WHERE (j.reference_type = 'CAPTURE' AND NOT EXISTS (SELECT 1 FROM captures c WHERE c.id = j.reference_id AND c.status = 'SUCCEEDED')) "
            + "OR (j.reference_type = 'REFUND' AND NOT EXISTS (SELECT 1 FROM refunds r WHERE r.id = j.reference_id AND r.status = 'SUCCEEDED'))"),
        new Check("DUPLICATE_JOURNAL", "SELECT NULL::uuid, reference_id, count(*) || ' journals for ' || reference_type FROM ledger_journals WHERE reference_type <> 'REVERSAL' GROUP BY reference_type, reference_id HAVING count(*) > 1"),
        new Check("UNBALANCED_JOURNAL", "SELECT NULL::uuid, journal_id, 'debits ' || sum(debit_minor) || ' <> credits ' || sum(credit_minor) FROM ledger_entries GROUP BY journal_id, currency HAVING sum(debit_minor) <> sum(credit_minor)"),
        new Check("CAPTURED_AMOUNT_MISMATCH", "SELECT p.id, NULL::uuid, 'captured_minor ' || p.captured_minor || ' vs succeeded captures ' || COALESCE((SELECT sum(c.amount_minor) FROM captures c WHERE c.payment_id = p.id AND c.status = 'SUCCEEDED'), 0) FROM payments p "
            + "WHERE p.captured_minor <> COALESCE((SELECT sum(c.amount_minor) FROM captures c WHERE c.payment_id = p.id AND c.status = 'SUCCEEDED'), 0)"),
        new Check("CAPTURE_STATE_MISMATCH", "SELECT p.id, c.id, 'capture SUCCEEDED but payment is ' || p.status FROM payments p JOIN captures c ON c.payment_id = p.id AND c.status = 'SUCCEEDED' WHERE p.status <> 'CAPTURED'"),
        new Check("REFUNDED_AMOUNT_MISMATCH", "SELECT p.id, NULL::uuid, 'refunded_minor ' || p.refunded_minor || ' vs pending+succeeded refunds ' || COALESCE((SELECT sum(r.amount_minor) FROM refunds r WHERE r.payment_id = p.id AND r.status IN ('PENDING','SUCCEEDED')), 0) FROM payments p "
            + "WHERE p.refunded_minor <> COALESCE((SELECT sum(r.amount_minor) FROM refunds r WHERE r.payment_id = p.id AND r.status IN ('PENDING','SUCCEEDED')), 0)"),
        new Check("PROVIDER_CAPTURE_MISMATCH", "SELECT c.payment_id, c.id, 'capture ' || c.status || ' vs provider ' || COALESCE(po.outcome, 'NOT_RECORDED') FROM captures c LEFT JOIN provider_operations po ON po.operation_id = 'cap-' || c.id "
            + "WHERE (c.status = 'SUCCEEDED' AND po.outcome IS DISTINCT FROM 'SUCCEEDED') OR (c.status = 'FAILED' AND po.outcome = 'SUCCEEDED')"),
        new Check("PROVIDER_REFUND_MISMATCH", "SELECT r.payment_id, r.id, 'refund ' || r.status || ' vs provider ' || COALESCE(po.outcome, 'NOT_RECORDED') FROM refunds r LEFT JOIN provider_operations po ON po.operation_id = 'ref-' || r.id "
            + "WHERE (r.status = 'SUCCEEDED' AND po.outcome IS DISTINCT FROM 'SUCCEEDED') OR (r.status = 'FAILED' AND po.outcome = 'SUCCEEDED')"),
        new Check("PROVIDER_OUTCOME_NOT_APPLIED", "SELECT c.payment_id, c.id, 'capture still PENDING but provider recorded ' || po.outcome FROM captures c JOIN provider_operations po ON po.operation_id = 'cap-' || c.id WHERE c.status = 'PENDING' "
            + "UNION ALL SELECT r.payment_id, r.id, 'refund still PENDING but provider recorded ' || po.outcome FROM refunds r JOIN provider_operations po ON po.operation_id = 'ref-' || r.id WHERE r.status = 'PENDING'"),
        new Check("STUCK_PAYMENT", "SELECT id, NULL::uuid, 'payment PROCESSING since ' || updated_at FROM payments WHERE status = 'PROCESSING' AND updated_at < " + STUCK),
        new Check("STUCK_CAPTURE", "SELECT payment_id, id, 'capture PENDING since ' || created_at FROM captures WHERE status = 'PENDING' AND created_at < " + STUCK),
        new Check("STUCK_REFUND", "SELECT payment_id, id, 'refund PENDING since ' || created_at FROM refunds WHERE status = 'PENDING' AND created_at < " + STUCK));

    private final JdbcTemplate jdbc;
    private final Props props;
    private final Outbox outbox;
    private final MeterRegistry metrics;
    private final TransactionTemplate tx;

    public ReconciliationService(JdbcTemplate jdbc, Props props, Outbox outbox, MeterRegistry metrics, PlatformTransactionManager tm) {
        this.tx = new TransactionTemplate(tm);
        this.jdbc = jdbc;
        this.props = props;
        this.outbox = outbox;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${paycore.reconciliation.interval}", initialDelayString = "PT1M")
    public void scheduled() {
        if (props.roles().webhook() && props.reconciliation().scheduleEnabled()) {
            try {
                run(false);
            } catch (Exception ignored) {
                // another instance is running it, or the database is briefly unavailable; the next tick retries
            }
        }
    }

    /** Explicit transaction (the scheduler calls this on `this`, so an annotation would be skipped and the advisory lock released at once). */
    public UUID run(boolean repair) {
        return tx.execute(s -> runInTransaction(repair));
    }

    private UUID runInTransaction(boolean repair) {
        Boolean got = jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(918273645)", Boolean.class);
        if (got == null || !got) {
            throw ApiException.conflict("reconciliation_running", "A reconciliation run is already in progress");
        }
        UUID runId = UUID.randomUUID();
        jdbc.update("INSERT INTO reconciliation_runs (id, status) VALUES (?, 'RUNNING')", runId);
        long stuckSeconds = props.reconciliation().stuckAfter().toSeconds();
        int found = 0;
        List<Map<String, Object>> repairable = new ArrayList<>();
        for (Check c : CHECKS) {
            Object[] args = c.sql().contains(STUCK) ? new Object[] {stuckSeconds} : new Object[0];
            // Read by column POSITION: several checks select two columns that are both called "id", which a Map would collapse.
            List<Object[]> rows = jdbc.query(c.sql() + " LIMIT 1000", (rs, i) -> new Object[] {rs.getObject(1), rs.getObject(2), rs.getString(3)}, args);
            for (Object[] v : rows) {
                jdbc.update("INSERT INTO reconciliation_discrepancies (id, run_id, type, payment_id, reference_id, details) VALUES (?,?,?,?,?,?)",
                        UUID.randomUUID(), runId, c.type(), v[0], v[1], abbreviate(String.valueOf(v[2])));
                found++;
                if (c.type().startsWith("STUCK_") || c.type().equals("PROVIDER_OUTCOME_NOT_APPLIED")) {
                    repairable.add(Map.of("type", c.type(), "payment", v[0] == null ? "" : v[0], "ref", v[1] == null ? "" : v[1]));
                }
            }
        }
        if (repair) {
            requeue(repairable);
        }
        Integer checked = jdbc.queryForObject("SELECT count(*) FROM payments", Integer.class);
        jdbc.update("UPDATE reconciliation_runs SET status = 'COMPLETED', completed_at = now(), payments_checked = ?, discrepancy_count = ? WHERE id = ?", checked, found, runId);
        outbox.enqueue(props.reconciliationEventsTopic(), "reconciliation.completed", runId, Map.of("runId", runId.toString(), "discrepancies", found));
        metrics.counter("paycore.reconciliation.discrepancies").increment(found);
        return runId;
    }

    /** Safe repair: re-queue the ORIGINAL command for work that is still unfinished. Handlers are idempotent, so this cannot double-apply. */
    private void requeue(List<Map<String, Object>> items) {
        for (Map<String, Object> i : items) {
            String type = (String) i.get("type");
            Object payment = i.get("payment");
            Object ref = i.get("ref");
            if (payment == null || payment.toString().isEmpty()) {
                continue;
            }
            UUID paymentId = UUID.fromString(payment.toString());
            UUID merchant = jdbc.queryForObject("SELECT merchant_id FROM payments WHERE id = ?", UUID.class, paymentId);
            boolean capture = type.equals("STUCK_CAPTURE") || (type.equals("PROVIDER_OUTCOME_NOT_APPLIED") && jdbc.queryForObject("SELECT count(*) FROM captures WHERE id = ?::uuid AND status = 'PENDING'", Integer.class, ref.toString()) > 0);
            if (type.equals("STUCK_PAYMENT")) {
                outbox.enqueue(props.commandsTopic(), "payment.authorize.requested", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString()));
            } else if (capture) {
                outbox.enqueue(props.commandsTopic(), "payment.capture.requested", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(), "captureId", ref.toString()));
            } else {
                outbox.enqueue(props.commandsTopic(), "payment.refund.requested", paymentId, Map.of("paymentId", paymentId.toString(), "merchantId", merchant.toString(), "refundId", ref.toString()));
            }
        }
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> runs() {
        return Camel.list(jdbc.queryForList("SELECT id, started_at, completed_at, status, payments_checked, discrepancy_count FROM reconciliation_runs ORDER BY started_at DESC LIMIT 100"));
    }

    @Transactional(readOnly = true)
    public Map<String, Object> run(UUID id) {
        List<Map<String, Object>> r = jdbc.queryForList("SELECT id, started_at, completed_at, status, payments_checked, discrepancy_count FROM reconciliation_runs WHERE id = ?", id);
        if (r.isEmpty()) {
            throw ApiException.notFound("reconciliation run");
        }
        Map<String, Object> out = new java.util.LinkedHashMap<>(Camel.map(r.get(0)));
        out.put("discrepancies", Camel.list(jdbc.queryForList("SELECT type, payment_id, reference_id, details FROM reconciliation_discrepancies WHERE run_id = ? ORDER BY type, created_at LIMIT 1000", id)));
        return out;
    }

    private static String abbreviate(String s) {
        return s.length() <= 500 ? s : s.substring(0, 500);
    }
}
