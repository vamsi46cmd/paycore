package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Ledger and payment integrity under concurrent load across many payments. */
class ConcurrencyIT extends TestSupport {
    @Test
    void manyPaymentsCapturedAndRefundedConcurrentlyKeepTheLedgerExact() throws Exception {
        Merchant m = newMerchant();
        int n = 20;
        List<UUID> payments = new ArrayList<>();
        long total = 0;
        for (int i = 0; i < n; i++) {
            long amount = 1000 + i * 37L;
            total += amount;
            UUID p = createPayment(m, "cust_load", amount);
            payments.add(p);
            authorize(m, p);
        }
        for (UUID p : payments) {
            awaitPayment(m, p, "AUTHORIZED");
        }
        // two competing capture requests (different keys) per payment, all released together
        List<Resp> captures = runConcurrently(2 * n, i -> () -> send("POST", "/api/v1/payments/" + payments.get(i / 2) + "/capture", m.apiKey(), Map.of(), "Idempotency-Key", UUID.randomUUID().toString()));
        assertEquals(n, captures.stream().filter(r -> r.status() == 202).count(), "one winner per payment");
        assertEquals(n, captures.stream().filter(r -> r.status() == 409).count());
        for (UUID p : payments) {
            awaitPayment(m, p, "CAPTURED");
        }
        assertEquals(n, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'CAPTURE' AND merchant_id = ?", m.id()), "one journal per payment");
        assertEquals(total, expect(200, send("GET", "/api/v1/ledger/balance", m.apiKey(), null)).body().get("merchantPayableMinor").asLong(), "ledger equals the sum of captures exactly");

        // two competing 60% refunds per payment: exactly one fits
        List<Resp> refunds = runConcurrently(2 * n, i -> () -> refund(m, payments.get(i / 2), (long) Math.floor(payment(m, payments.get(i / 2)).get("capturedMinor").asLong() * 0.6), "r-" + i));
        assertEquals(n, refunds.stream().filter(r -> r.status() == 202).count());
        assertEquals(n, refunds.stream().filter(r -> r.status() == 422).count());
        await("refunds settled", Duration.ofSeconds(60), () -> count("SELECT count(*) FROM refunds r JOIN payments p ON p.id = r.payment_id WHERE p.merchant_id = ? AND r.status = 'PENDING'", m.id()), c -> c == 0);

        long refunded = jdbc.queryForObject("SELECT COALESCE(SUM(r.amount_minor), 0) FROM refunds r JOIN payments p ON p.id = r.payment_id WHERE p.merchant_id = ? AND r.status = 'SUCCEEDED'", Long.class, m.id());
        assertEquals(total - refunded, expect(200, send("GET", "/api/v1/ledger/balance", m.apiKey(), null)).body().get("merchantPayableMinor").asLong());
        assertEquals(0, count("SELECT count(*) FROM payments WHERE merchant_id = ? AND refunded_minor > captured_minor", m.id()), "no payment over-refunded");
        assertEquals(0, count("SELECT count(*) FROM (SELECT journal_id FROM ledger_entries GROUP BY journal_id, currency HAVING sum(debit_minor) <> sum(credit_minor)) x"), "no unbalanced journal exists anywhere");
        assertEquals(0, count("SELECT count(*) FROM (SELECT reference_type, reference_id FROM ledger_journals GROUP BY 1, 2 HAVING count(*) > 1) x"), "no duplicate postings");
        assertEquals(0, count("SELECT count(*) FROM payments p WHERE p.merchant_id = ? AND p.refunded_minor <> COALESCE((SELECT sum(r.amount_minor) FROM refunds r WHERE r.payment_id = p.id AND r.status IN ('PENDING','SUCCEEDED')), 0)", m.id()));
    }
}
