package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Reconciliation against INTENTIONALLY injected inconsistencies (written with raw SQL, bypassing the application). */
class ReconciliationIT extends TestSupport {
    @Autowired Ledger ledger;
    @Autowired PlatformTransactionManager tm;

    private UUID payment(Merchant m, String status, long amount, long authorized, long captured, long refunded) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, authorized_minor, captured_minor, refunded_minor, idempotency_key) VALUES (?,?, 'c', ?, 'USD', ?, ?, ?, ?, ?)",
                id, m.id(), amount, status, authorized, captured, refunded, UUID.randomUUID().toString());
        return id;
    }

    private UUID capture(UUID payment, long amount, String status, String createdAgo) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO captures (id, payment_id, amount_minor, status, idempotency_key, created_at) VALUES (?,?,?,?,?, now() - ?::interval)", id, payment, amount, status, UUID.randomUUID().toString(), createdAgo);
        return id;
    }

    private UUID refundRow(UUID payment, long amount, String status, String createdAgo) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO refunds (id, payment_id, amount_minor, status, idempotency_key, created_at) VALUES (?,?,?,?,?, now() - ?::interval)", id, payment, amount, status, UUID.randomUUID().toString(), createdAgo);
        return id;
    }

    private void provider(String op, String type, UUID payment, long amount, String outcome) {
        jdbc.update("INSERT INTO provider_operations (operation_id, op_type, payment_id, amount_minor, outcome) VALUES (?,?,?,?,?)", op, type, payment, amount, outcome);
    }

    private void journalCapture(Merchant m, UUID captureId, long amount) {
        new TransactionTemplate(tm).executeWithoutResult(s -> ledger.postCapture(m.id(), captureId, amount));
    }

    private UUID run(boolean repair) {
        return UUID.fromString(expect(201, send("POST", "/api/v1/reconciliation/runs?repair=" + repair, ADMIN_KEY, null)).body().get("id").asText());
    }

    private Set<String> findings(UUID run, UUID payment, UUID... references) {
        Set<String> out = new HashSet<>(jdbc.queryForList("SELECT type FROM reconciliation_discrepancies WHERE run_id = ? AND payment_id = ?", String.class, run, payment));
        for (UUID ref : references) {
            out.addAll(jdbc.queryForList("SELECT type FROM reconciliation_discrepancies WHERE run_id = ? AND reference_id = ?", String.class, run, ref));
        }
        return out;
    }

    /** A fully consistent captured payment: payment, capture, provider record and ledger journal all agree. */
    private UUID consistent(Merchant m, long amount) {
        UUID p = payment(m, "CAPTURED", amount, amount, amount, 0);
        UUID c = capture(p, amount, "SUCCEEDED", "1 minute");
        provider("cap-" + c, "CAPTURE", p, amount, "SUCCEEDED");
        journalCapture(m, c, amount);
        return p;
    }

    @Test
    void aConsistentPaymentProducesNoFindings() {
        Merchant m = newMerchant();
        UUID p = consistent(m, 1000);
        assertEquals(Set.of(), findings(run(false), p));
    }

    @Test
    void detectsACapturedPaymentWithNoLedgerJournal() {
        Merchant m = newMerchant();
        UUID p = payment(m, "CAPTURED", 1000, 1000, 1000, 0);
        UUID c = capture(p, 1000, "SUCCEEDED", "1 minute");
        provider("cap-" + c, "CAPTURE", p, 1000, "SUCCEEDED");
        Set<String> f = findings(run(false), p, c);
        assertTrue(f.contains("MISSING_CAPTURE_JOURNAL"), f.toString());
        assertFalse(f.contains("PROVIDER_CAPTURE_MISMATCH"));
    }

    @Test
    void detectsALedgerAmountThatDiffersFromTheCapture() {
        Merchant m = newMerchant();
        UUID p = payment(m, "CAPTURED", 1000, 1000, 1000, 0);
        UUID c = capture(p, 1000, "SUCCEEDED", "1 minute");
        provider("cap-" + c, "CAPTURE", p, 1000, "SUCCEEDED");
        journalCapture(m, c, 900);   // the ledger says 900, the capture says 1000
        assertTrue(findings(run(false), p, c).contains("CAPTURE_LEDGER_AMOUNT_MISMATCH"));
    }

    @Test
    void detectsCapturedAmountAndStateMismatches() {
        Merchant m = newMerchant();
        UUID p = payment(m, "CAPTURED", 1000, 1000, 500, 0);   // payment says 500 captured
        UUID c = capture(p, 1000, "SUCCEEDED", "1 minute");     // but the capture was 1000
        provider("cap-" + c, "CAPTURE", p, 1000, "SUCCEEDED");
        journalCapture(m, c, 1000);
        assertTrue(findings(run(false), p, c).contains("CAPTURED_AMOUNT_MISMATCH"));
        UUID q = payment(m, "AUTHORIZED", 700, 700, 0, 0);       // capture succeeded but the payment never became CAPTURED
        UUID c2 = capture(q, 700, "SUCCEEDED", "1 minute");
        provider("cap-" + c2, "CAPTURE", q, 700, "SUCCEEDED");
        assertTrue(findings(run(false), q, c2).contains("CAPTURE_STATE_MISMATCH"));
    }

    @Test
    void detectsRefundInconsistencies() {
        Merchant m = newMerchant();
        UUID p = consistent(m, 1000);
        UUID r = refundRow(p, 100, "SUCCEEDED", "1 minute");   // succeeded, but: no journal, and payment.refunded_minor is still 0
        provider("ref-" + r, "REFUND", p, 100, "SUCCEEDED");
        Set<String> f = findings(run(false), p, r);
        assertTrue(f.contains("MISSING_REFUND_JOURNAL"), f.toString());
        assertTrue(f.contains("REFUNDED_AMOUNT_MISMATCH"), f.toString());
    }

    @Test
    void detectsStuckWork() {
        Merchant m = newMerchant();
        UUID processing = payment(m, "PROCESSING", 100, 0, 0, 0);
        jdbc.update("UPDATE payments SET updated_at = now() - interval '1 hour' WHERE id = ?", processing);
        UUID authorized = payment(m, "AUTHORIZED", 300, 300, 0, 0);
        UUID pendingCapture = capture(authorized, 300, "PENDING", "1 hour");
        UUID captured = payment(m, "CAPTURED", 500, 500, 500, 200);
        UUID cap = capture(captured, 500, "SUCCEEDED", "2 hours");
        provider("cap-" + cap, "CAPTURE", captured, 500, "SUCCEEDED");
        journalCapture(m, cap, 500);
        UUID pendingRefund = refundRow(captured, 200, "PENDING", "1 hour");
        UUID run = run(false);
        assertTrue(findings(run, processing).contains("STUCK_PAYMENT"));
        assertTrue(findings(run, authorized, pendingCapture).contains("STUCK_CAPTURE"));
        assertTrue(findings(run, captured, pendingRefund).contains("STUCK_REFUND"));
    }

    @Test
    void detectsProviderOutcomeMismatches() {
        Merchant m = newMerchant();
        UUID p = payment(m, "CAPTURED", 400, 400, 400, 0);
        UUID c = capture(p, 400, "SUCCEEDED", "1 minute");
        provider("cap-" + c, "CAPTURE", p, 400, "FAILED");   // we think it succeeded, the provider says it failed
        journalCapture(m, c, 400);
        UUID q = payment(m, "AUTHORIZED", 600, 600, 0, 0);
        UUID c2 = capture(q, 600, "FAILED", "1 minute");
        provider("cap-" + c2, "CAPTURE", q, 600, "SUCCEEDED");   // we think it failed, the provider captured it
        UUID run = run(false);
        assertTrue(findings(run, p, c).contains("PROVIDER_CAPTURE_MISMATCH"));
        assertTrue(findings(run, q, c2).contains("PROVIDER_CAPTURE_MISMATCH"));
    }

    @Test
    void detectsALedgerJournalWhoseSourceNeverSucceeded() {
        Merchant m = newMerchant();
        UUID p = payment(m, "AUTHORIZED", 250, 250, 0, 0);
        UUID c = capture(p, 250, "FAILED", "1 minute");
        journalCapture(m, c, 250);
        assertTrue(findings(run(false), p, c).contains("JOURNAL_WITHOUT_SUCCESSFUL_SOURCE"));
    }

    @Test
    void safeRepairRequeuesStuckWorkAndNeverTouchesMoney() throws Exception {
        Merchant m = newMerchant();
        UUID p = payment(m, "AUTHORIZED", 900, 900, 0, 0);
        UUID c = capture(p, 900, "PENDING", "1 hour");              // its command was lost...
        provider("cap-" + c, "CAPTURE", p, 900, "SUCCEEDED");        // ...but the provider did capture
        UUID report = run(false);
        assertTrue(findings(report, p, c).containsAll(Set.of("STUCK_CAPTURE", "PROVIDER_OUTCOME_NOT_APPLIED")));
        assertEquals(0, count("SELECT count(*) FROM ledger_journals WHERE reference_id = ?", c), "a plain run repairs nothing");

        run(true);   // re-queues the original command; the idempotent worker finishes the job
        await("capture completed by the worker", Duration.ofSeconds(40), () -> jdbc.queryForObject("SELECT status FROM captures WHERE id = ?", String.class, c), "SUCCEEDED"::equals);
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'CAPTURE' AND reference_id = ?", c), "exactly one journal, posted by the worker (not by reconciliation)");
        assertEquals("CAPTURED", jdbc.queryForObject("SELECT status FROM payments WHERE id = ?", String.class, p));
        assertEquals(Set.of(), findings(run(false), p, c), "the discrepancy is gone");
    }

    @Test
    void reversingAJournalCorrectsTheBalanceWithoutRewritingHistory() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_reverse", 1000);
        UUID captureId = jdbc.queryForObject("SELECT id FROM captures WHERE payment_id = ?", UUID.class, p);
        UUID original = jdbc.queryForObject("SELECT id FROM ledger_journals WHERE reference_type = 'CAPTURE' AND reference_id = ?", UUID.class, captureId);
        List<String> before = jdbc.queryForList("SELECT id || ':' || debit_minor || ':' || credit_minor FROM ledger_entries WHERE journal_id = ? ORDER BY id", String.class, original);
        assertEquals(1000, ledger.merchantPayableBalance(m.id()));

        UUID reversal = new TransactionTemplate(tm).execute(s -> ledger.reverse(original, "customer dispute"));
        assertEquals(0, ledger.merchantPayableBalance(m.id()), "credits 1000 - debits 1000");
        assertEquals(before, jdbc.queryForList("SELECT id || ':' || debit_minor || ':' || credit_minor FROM ledger_entries WHERE journal_id = ? ORDER BY id", String.class, original), "the original journal is untouched");
        assertEquals(original, jdbc.queryForObject("SELECT reverses_journal_id FROM ledger_journals WHERE id = ?", UUID.class, reversal));
        assertEquals(reversal, new TransactionTemplate(tm).execute(s -> ledger.reverse(original, "again")), "reversing twice returns the same reversal");
        assertEquals(2, count("SELECT count(*) FROM ledger_journals WHERE merchant_id = ?", m.id()));
        assertThrows(DataAccessException.class, () -> jdbc.update("DELETE FROM ledger_journals WHERE id = ?", reversal));
        // the reversal itself balances: no unbalanced journals anywhere
        assertEquals(0, count("SELECT count(*) FROM (SELECT journal_id FROM ledger_entries GROUP BY journal_id, currency HAVING sum(debit_minor) <> sum(credit_minor)) x"));
    }
}
