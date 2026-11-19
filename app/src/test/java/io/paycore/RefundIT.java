package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RefundIT extends TestSupport {
    private long payable(Merchant m) {
        return expect(200, send("GET", "/api/v1/ledger/balance", m.apiKey(), null)).body().get("merchantPayableMinor").asLong();
    }

    @Test
    void fullRefundBringsThePayableBackToZero() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_full", 1000);
        Resp r = refund(m, p, 1000, "k-full");
        assertEquals(202, r.status());
        awaitRefund(UUID.fromString(r.body().get("id").asText()), "SUCCEEDED");
        assertEquals(1000, payment(m, p).get("refundedMinor").asLong());
        assertEquals(0, payable(m), "captured 1000, refunded 1000");
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'REFUND' AND merchant_id = ?", m.id()));
        assertEquals(422, refund(m, p, 1, "k-extra").status(), "nothing left to refund");
    }

    @Test
    void multiplePartialRefundsUpToTheCapturedAmount() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_partials", 1000);
        for (int i = 0; i < 3; i++) {
            long amt = i == 2 ? 300 : (i == 0 ? 300 : 400);
            Resp r = refund(m, p, amt, "k-part-" + i);
            assertEquals(202, r.status(), r.body().toString());
            awaitRefund(UUID.fromString(r.body().get("id").asText()), "SUCCEEDED");
        }
        assertEquals(1000, payment(m, p).get("refundedMinor").asLong());
        assertEquals(3, expect(200, send("GET", "/api/v1/payments/" + p + "/refunds", m.apiKey(), null)).body().size());
        assertEquals(0, payable(m));
        assertEquals(422, refund(m, p, 1, "k-over").status());
    }

    @Test
    void duplicateRefundRequestIsReplayedNotRepeated() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_dupref", 1000);
        Resp a = refund(m, p, 400, "k-dup");
        Resp b = refund(m, p, 400, "k-dup");
        assertEquals(202, a.status());
        assertEquals(202, b.status());
        assertEquals(a.body().get("id").asText(), b.body().get("id").asText());
        assertEquals("true", b.headers().firstValue("Idempotent-Replayed").orElse(null));
        awaitRefund(UUID.fromString(a.body().get("id").asText()), "SUCCEEDED");
        assertEquals(409, refund(m, p, 500, "k-dup").status(), "same key, different amount");
        assertEquals(1, count("SELECT count(*) FROM refunds WHERE payment_id = ?", p));
        assertEquals(400, payment(m, p).get("refundedMinor").asLong(), "refunded once, not twice");
        assertEquals(600, payable(m));
    }

    @Test
    void refundValidationAndStateRules() throws Exception {
        Merchant m = newMerchant();
        UUID created = createPayment(m, "cust_v", 500);
        assertEquals(409, refund(m, created, 100, "k1").status(), "uncaptured payment");
        UUID authorized = createPayment(m, "cust_v2", 500);
        authorize(m, authorized);
        awaitPayment(m, authorized, "AUTHORIZED");
        Resp r = refund(m, authorized, 100, "k2");
        assertEquals(409, r.status());
        assertEquals("payment_not_captured", r.body().get("code").asText());
        UUID captured = capturedPayment(m, "cust_v3", 500);
        Resp over = refund(m, captured, 501, "k3");
        assertEquals(422, over.status());
        assertEquals("refund_exceeds_remaining", over.body().get("code").asText());
        assertEquals(400, refund(m, captured, 0, "k4").status());
        assertEquals(400, refund(m, captured, -5, "k5").status());
        assertEquals(404, refund(m, UUID.randomUUID(), 100, "k6").status());
        assertEquals(0, payment(m, captured).get("refundedMinor").asLong(), "rejected refunds reserve nothing");
        assertEquals(0, count("SELECT count(*) FROM refunds WHERE payment_id IN (?, ?, ?)", created, authorized, captured));
    }

    @Test
    void twoConcurrentRefundsCannotTogetherExceedTheCapturedAmount() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_race2", 1000);
        List<Resp> rs = runConcurrently(2, i -> () -> refund(m, p, 700, "race-" + i));
        assertEquals(1, rs.stream().filter(r -> r.status() == 202).count(), "exactly one 700 refund fits into 1000");
        assertEquals(1, rs.stream().filter(r -> r.status() == 422).count());
        assertEquals(700, payment(m, p).get("refundedMinor").asLong());
    }

    @Test
    void manyConcurrentRefundsNeverOverRefund() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_race10", 1000);
        List<Resp> rs = runConcurrently(12, i -> () -> refund(m, p, 150, "many-" + i));
        long accepted = rs.stream().filter(r -> r.status() == 202).count();
        assertEquals(6, accepted, "1000 / 150 = 6 whole refunds fit");
        rs.stream().filter(r -> r.status() != 202).forEach(r -> assertEquals(422, r.status()));
        assertEquals(900, payment(m, p).get("refundedMinor").asLong());
        await("all refunds settled", java.time.Duration.ofSeconds(40), () -> count("SELECT count(*) FROM refunds WHERE payment_id = ? AND status = 'PENDING'", p), n -> n == 0);
        assertEquals(6, count("SELECT count(*) FROM refunds WHERE payment_id = ? AND status = 'SUCCEEDED'", p));
        assertEquals(100, payable(m), "1000 captured - 900 refunded, from the ledger");
        assertEquals(6, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'REFUND' AND merchant_id = ?", m.id()));
    }

    @Test
    void aFailedRefundIsNotRecordedAsSuccessAndReleasesItsReservation() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_refund_fail", 1000);
        Resp r = refund(m, p, 400, "k-fail");
        assertEquals(202, r.status());
        UUID refundId = UUID.fromString(r.body().get("id").asText());
        assertEquals("FAILED", awaitRefund(refundId, "FAILED"));
        await("reservation released", java.time.Duration.ofSeconds(10), () -> payment(m, p).get("refundedMinor").asLong(), v -> v == 0);
        assertEquals(0, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'REFUND' AND merchant_id = ?", m.id()), "no ledger posting for a failed refund");
        assertEquals(1000, payable(m), "merchant balance untouched");
        assertEquals("refund_declined", jdbc.queryForObject("SELECT failure_code FROM refunds WHERE id = ?", String.class, refundId));
    }

    @Test
    void ambiguousProviderOutcomeIsResolvedByLookupNotBlindRetry() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_timeout_after_success", 1000);   // the capture itself was ambiguous too
        String capOp = jdbc.queryForObject("SELECT 'cap-' || id FROM captures WHERE payment_id = ?", String.class, p);
        assertEquals(1, count("SELECT calls FROM provider_call_log WHERE operation_id = ?", capOp), "worker looked the outcome up instead of calling the provider again");
        Resp r = refund(m, p, 250, "k-amb");
        UUID refundId = UUID.fromString(r.body().get("id").asText());
        awaitRefund(refundId, "SUCCEEDED");
        assertEquals(1, count("SELECT calls FROM provider_call_log WHERE operation_id = ?", "ref-" + refundId), "one provider call, no blind retry");
        assertEquals(1, count("SELECT count(*) FROM provider_operations WHERE operation_id = ?", "ref-" + refundId));
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'REFUND' AND reference_id = ?", refundId));
        assertEquals(750, payable(m));
    }
}
