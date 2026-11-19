package io.paycore;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Double-entry ledger over PostgreSQL. All amounts are integer minor units.
 *
 * Accounts and sign conventions
 *  - SETTLEMENT_CLEARING (one, asset, debit-normal): balance = debits - credits. Value the platform is owed by the (simulated) provider.
 *  - MERCHANT_PAYABLE (one per merchant, liability, credit-normal): balance = credits - debits. What the platform owes the merchant.
 * Postings (illustrative simulation conventions)
 *  - Capture of N : DEBIT clearing N, CREDIT merchant payable N.
 *  - Refund  of N : DEBIT merchant payable N, CREDIT clearing N.
 *  - Reversal of journal J: a NEW journal with every entry's debit/credit swapped, linked by reverses_journal_id.
 * Invariants live in PostgreSQL: balanced-per-currency deferred trigger, immutable rows, UNIQUE (reference_type, reference_id)
 * so replays and duplicate events cannot post twice. There is no stored balance column; balances are derived from entries,
 * so postings never contend on a hot row.
 */
@Service
public class Ledger {
    private final JdbcTemplate jdbc;

    public Ledger(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID postCapture(UUID merchantId, UUID captureId, long amountMinor) {
        UUID clearing = account("SETTLEMENT_CLEARING", null);
        UUID payable = account("MERCHANT_PAYABLE", merchantId);
        return post("CAPTURE", captureId, merchantId, null, "capture", clearing, payable, amountMinor);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public UUID postRefund(UUID merchantId, UUID refundId, long amountMinor) {
        UUID clearing = account("SETTLEMENT_CLEARING", null);
        UUID payable = account("MERCHANT_PAYABLE", merchantId);
        return post("REFUND", refundId, merchantId, null, "refund", payable, clearing, amountMinor);
    }

    /** Corrects a posted journal by posting its mirror image; the original is never touched. Idempotent per original journal. */
    @Transactional(propagation = Propagation.MANDATORY)
    public UUID reverse(UUID journalId, String reason) {
        UUID existing = jdbc.query("SELECT id FROM ledger_journals WHERE reverses_journal_id = ?", rs -> rs.next() ? rs.getObject(1, UUID.class) : null, journalId);
        if (existing != null) {
            return existing;
        }
        Map<String, Object> j = jdbc.queryForMap("SELECT merchant_id FROM ledger_journals WHERE id = ?", journalId);
        UUID newId = UUID.randomUUID();
        jdbc.update("INSERT INTO ledger_journals (id, reference_type, reference_id, merchant_id, reverses_journal_id, description) VALUES (?, 'REVERSAL', ?, ?, ?, ?)",
                newId, journalId, j.get("merchant_id"), journalId, abbreviate("reversal: " + reason));
        jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) "
                + "SELECT gen_random_uuid(), ?, account_id, credit_minor, debit_minor, currency FROM ledger_entries WHERE journal_id = ?", newId, journalId);
        return newId;
    }

    private UUID post(String type, UUID referenceId, UUID merchantId, UUID reverses, String description, UUID debitAccount, UUID creditAccount, long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("ledger amount must be positive");
        }
        UUID journalId = UUID.randomUUID();
        List<UUID> inserted = jdbc.query("INSERT INTO ledger_journals (id, reference_type, reference_id, merchant_id, reverses_journal_id, description) VALUES (?,?,?,?,?,?) "
                + "ON CONFLICT (reference_type, reference_id) DO NOTHING RETURNING id", (rs, i) -> rs.getObject(1, UUID.class),
                journalId, type, referenceId, merchantId, reverses, description);
        if (inserted.isEmpty()) {
            // Already posted (replay / duplicate event): return the original journal, post nothing.
            return jdbc.queryForObject("SELECT id FROM ledger_journals WHERE reference_type = ? AND reference_id = ?", UUID.class, type, referenceId);
        }
        jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,?,0,'USD')", UUID.randomUUID(), journalId, debitAccount, amount);
        jdbc.update("INSERT INTO ledger_entries (id, journal_id, account_id, debit_minor, credit_minor, currency) VALUES (?,?,?,0,?,'USD')", UUID.randomUUID(), journalId, creditAccount, amount);
        return journalId;
    }

    private UUID account(String type, UUID merchantId) {
        jdbc.update("INSERT INTO ledger_accounts (id, account_type, merchant_id, currency) VALUES (?,?,?,'USD') ON CONFLICT DO NOTHING", UUID.randomUUID(), type, merchantId);
        return jdbc.queryForObject("SELECT id FROM ledger_accounts WHERE account_type = ? AND merchant_id IS NOT DISTINCT FROM ? AND currency = 'USD'", UUID.class, type, merchantId);
    }

    /** What the platform owes the merchant, in minor units: credits - debits on the merchant payable account. */
    @Transactional(readOnly = true)
    public long merchantPayableBalance(UUID merchantId) {
        Long v = jdbc.queryForObject("SELECT COALESCE(SUM(e.credit_minor - e.debit_minor), 0) FROM ledger_entries e JOIN ledger_accounts a ON a.id = e.account_id "
                + "WHERE a.account_type = 'MERCHANT_PAYABLE' AND a.merchant_id = ?", Long.class, merchantId);
        return v == null ? 0 : v;
    }

    /** Debit-normal clearing balance: debits - credits. */
    @Transactional(readOnly = true)
    public long clearingBalance() {
        Long v = jdbc.queryForObject("SELECT COALESCE(SUM(e.debit_minor - e.credit_minor), 0) FROM ledger_entries e JOIN ledger_accounts a ON a.id = e.account_id "
                + "WHERE a.account_type = 'SETTLEMENT_CLEARING'", Long.class);
        return v == null ? 0 : v;
    }

    private static String abbreviate(String s) {
        return s.length() <= 200 ? s : s.substring(0, 200);
    }
}
