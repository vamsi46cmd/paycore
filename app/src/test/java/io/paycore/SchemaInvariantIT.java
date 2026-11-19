package io.paycore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Attacks the financial invariants directly with SQL, bypassing all application code, to prove PostgreSQL itself enforces them:
 * balanced journals, immutable ledger, unique postings, valid transitions, refund ceiling, single live capture.
 */
class SchemaInvariantIT extends TestSupport {
    @Autowired PlatformTransactionManager tm;

    private UUID merchant() {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO merchants (id, name, api_key_hash) VALUES (?,?,?)", id, "m", SecurityConfig.sha256(id.toString()));
        return id;
    }

    private UUID payment(UUID merchant, String status, long amount, long authorized, long captured) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, authorized_minor, captured_minor, idempotency_key) VALUES (?,?,?,?, 'USD', ?, ?, ?, ?)",
                id, merchant, "c", amount, status, authorized, captured, UUID.randomUUID().toString());
        return id;
    }

    private UUID account(UUID merchant) {
        UUID a = UUID.randomUUID();
        jdbc.update("INSERT INTO ledger_accounts (id, account_type, merchant_id, currency) VALUES (?, 'MERCHANT_PAYABLE', ?, 'USD') ON CONFLICT DO NOTHING", a, merchant);
        return jdbc.queryForObject("SELECT id FROM ledger_accounts WHERE account_type = 'MERCHANT_PAYABLE' AND merchant_id = ?", UUID.class, merchant);
    }

    private void journal(UUID merchant, UUID accountDebit, UUID accountCredit, long debit, long credit, UUID reference) {
        new TransactionTemplate(tm).executeWithoutResult(s -> {
            UUID j = UUID.randomUUID();
            jdbc.update("INSERT INTO ledger_journals (id, reference_type, reference_id, merchant_id) VALUES (?, 'CAPTURE', ?, ?)", j, reference, merchant);
            jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,?,0,'USD')", UUID.randomUUID(), j, accountDebit, debit);
            jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,0,?,'USD')", UUID.randomUUID(), j, accountCredit, credit);
        });
    }

    private UUID clearing() {
        jdbc.update("INSERT INTO ledger_accounts (id, account_type, merchant_id, currency) VALUES (?, 'SETTLEMENT_CLEARING', NULL, 'USD') ON CONFLICT DO NOTHING", UUID.randomUUID());
        return jdbc.queryForObject("SELECT id FROM ledger_accounts WHERE account_type = 'SETTLEMENT_CLEARING'", UUID.class);
    }

    private static void assertMessage(Throwable t, String fragment) {
        Throwable c = t;
        while (c != null) {
            if (c.getMessage() != null && c.getMessage().contains(fragment)) {
                return;
            }
            c = c.getCause();
        }
        throw new AssertionError("expected message containing '" + fragment + "' but got: " + t);
    }

    @Test
    void balancedJournalIsAccepted() {
        UUID m = merchant();
        assertDoesNotThrow(() -> journal(m, clearing(), account(m), 500, 500, UUID.randomUUID()));
    }

    @Test
    void unbalancedJournalCannotCommit() {
        UUID m = merchant();
        UUID ref = UUID.randomUUID();
        Throwable t = assertThrows(DataAccessException.class, () -> journal(m, clearing(), account(m), 100, 90, ref));
        assertMessage(t, "unbalanced");
        assertEquals(0, count("SELECT count(*) FROM ledger_journals WHERE reference_id = ?", ref), "the rejected journal left nothing behind");
    }

    @Test
    void journalNeedsAtLeastTwoEntries() {
        UUID m = merchant();
        UUID ref = UUID.randomUUID();
        UUID acct = account(m);
        assertThrows(DataAccessException.class, () -> new TransactionTemplate(tm).executeWithoutResult(s -> {
            UUID j = UUID.randomUUID();
            jdbc.update("INSERT INTO ledger_journals (id, reference_type, reference_id, merchant_id) VALUES (?, 'CAPTURE', ?, ?)", j, ref, m);
            jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,100,0,'USD')", UUID.randomUUID(), j, acct);
        }));
        assertThrows(DataAccessException.class, () -> new TransactionTemplate(tm).executeWithoutResult(s ->
            jdbc.update("INSERT INTO ledger_journals (id, reference_type, reference_id, merchant_id) VALUES (?, 'CAPTURE', ?, ?)", UUID.randomUUID(), UUID.randomUUID(), m)), "a journal with no entries");
    }

    @Test
    void ledgerRowsAreImmutable() {
        UUID m = merchant();
        UUID ref = UUID.randomUUID();
        journal(m, clearing(), account(m), 300, 300, ref);
        UUID journalId = jdbc.queryForObject("SELECT id FROM ledger_journals WHERE reference_id = ?", UUID.class, ref);
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE ledger_entries SET debit_minor = 1 WHERE journal_id = ? AND debit_minor > 0", journalId)), "immutable");
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("DELETE FROM ledger_entries WHERE journal_id = ?", journalId)), "immutable");
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE ledger_journals SET description = 'x' WHERE id = ?", journalId)), "immutable");
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("DELETE FROM ledger_journals WHERE id = ?", journalId)), "immutable");
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.execute("TRUNCATE ledger_entries")), "immutable");
        assertEquals(2, count("SELECT count(*) FROM ledger_entries WHERE journal_id = ?", journalId));
    }

    @Test
    void aFinancialEventCanBePostedOnlyOnce() {
        UUID m = merchant();
        UUID ref = UUID.randomUUID();
        journal(m, clearing(), account(m), 100, 100, ref);
        assertThrows(DataAccessException.class, () -> journal(m, clearing(), account(m), 100, 100, ref));
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_id = ?", ref));
    }

    @Test
    void entriesMustBeOneSidedPositiveUsd() {
        UUID m = merchant();
        UUID a = account(m);
        UUID j = UUID.randomUUID();
        // never committed: each statement is rejected by a CHECK constraint before any trigger runs
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,5,5,'USD')", UUID.randomUUID(), j, a));
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,0,0,'USD')", UUID.randomUUID(), j, a));
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,-5,0,'USD')", UUID.randomUUID(), j, a));
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,5,0,'EUR')", UUID.randomUUID(), j, a));
    }

    @Test
    void refundedAmountCanNeverExceedCaptured() {
        UUID p = payment(merchant(), "CAPTURED", 1000, 1000, 1000);
        assertDoesNotThrow(() -> jdbc.update("UPDATE payments SET refunded_minor = 1000 WHERE id = ?", p));
        assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET refunded_minor = 1001 WHERE id = ?", p));
        assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET refunded_minor = -1 WHERE id = ?", p));
        UUID q = payment(merchant(), "CREATED", 500, 0, 0);
        assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET refunded_minor = 1 WHERE id = ?", q), "nothing captured, nothing refundable");
        assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET captured_minor = 600, authorized_minor = 500 WHERE id = ?", q), "captured cannot exceed authorized");
    }

    @Test
    void invalidPaymentTransitionsAreRejectedByTheDatabase() {
        UUID m = merchant();
        UUID p = payment(m, "CREATED", 1000, 0, 0);
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET status = 'CAPTURED' WHERE id = ?", p)), "invalid payment transition");
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET status = 'AUTHORIZED' WHERE id = ?", p)), "invalid payment transition");
        assertDoesNotThrow(() -> jdbc.update("UPDATE payments SET status = 'PROCESSING' WHERE id = ?", p));
        assertDoesNotThrow(() -> jdbc.update("UPDATE payments SET status = 'FAILED' WHERE id = ?", p));
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET status = 'PROCESSING' WHERE id = ?", p)), "invalid payment transition");   // terminal
        UUID captured = payment(m, "CAPTURED", 1000, 1000, 1000);
        assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET status = 'AUTHORIZED' WHERE id = ?", captured));
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET captured_minor = 1 WHERE id = ?", captured)), "cannot decrease");
        assertMessage(assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET amount_minor = 5 WHERE id = ?", captured)), "immutable");
        assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE payments SET currency = 'EUR' WHERE id = ?", captured));
    }

    @Test
    void onlyOneLiveCapturePerPayment() {
        UUID p = payment(merchant(), "AUTHORIZED", 1000, 1000, 0);
        UUID first = UUID.randomUUID();
        jdbc.update("INSERT INTO captures (id, payment_id, amount_minor, status, idempotency_key) VALUES (?,?,1000,'PENDING','k1')", first, p);
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO captures (id, payment_id, amount_minor, status, idempotency_key) VALUES (?,?,1000,'PENDING','k2')", UUID.randomUUID(), p));
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO captures (id, payment_id, amount_minor, status, idempotency_key) VALUES (?,?,1000,'SUCCEEDED','k3')", UUID.randomUUID(), p));
        jdbc.update("UPDATE captures SET status = 'FAILED' WHERE id = ?", first);
        assertDoesNotThrow(() -> jdbc.update("INSERT INTO captures (id, payment_id, amount_minor, status, idempotency_key) VALUES (?,?,1000,'PENDING','k4')", UUID.randomUUID(), p), "a failed capture may be retried");
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO captures (id, payment_id, amount_minor, status, idempotency_key) VALUES (?,?,0,'FAILED','k5')", UUID.randomUUID(), p), "amounts must be positive");
    }

    @Test
    void paymentAmountsMustBePositiveUsdAndKeysUnique() {
        UUID m = merchant();
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, idempotency_key) VALUES (?,?,?,0,'USD','CREATED','a')", UUID.randomUUID(), m, "c"));
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, idempotency_key) VALUES (?,?,?,-5,'USD','CREATED','a')", UUID.randomUUID(), m, "c"));
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, idempotency_key) VALUES (?,?,?,5,'EUR','CREATED','a')", UUID.randomUUID(), m, "c"));
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, idempotency_key) VALUES (?,?,?,5,'USD','BOGUS','a')", UUID.randomUUID(), m, "c"));
        jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, idempotency_key) VALUES (?,?,?,5,'USD','CREATED','same')", UUID.randomUUID(), m, "c");
        assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, idempotency_key) VALUES (?,?,?,5,'USD','CREATED','same')", UUID.randomUUID(), m, "c"));
        assertTrue(true);
    }
}
