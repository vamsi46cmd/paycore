package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** End-to-end: merchant -> payment -> authorize -> capture -> ledger -> events -> webhooks -> partial refund -> reconciliation. */
class PaymentFlowIT extends TestSupport {
    @Test
    void completeHappyPath() throws Exception {
        // 1-2. merchant and authentication
        Merchant m = newMerchant();
        expect(401, send("GET", "/api/v1/payments", null, null));
        expect(200, send("GET", "/api/v1/payments", m.apiKey(), null));

        // webhook endpoint (the local test receiver verifies signatures)
        JsonNode ep = expect(201, send("POST", "/api/v1/webhooks/endpoints", m.apiKey(),
                Map.of("url", "http://localhost:" + port + "/internal/test-receiver", "eventTypes", List.of("*")))).body();
        UUID endpointId = UUID.fromString(ep.get("endpointId").asText());

        // 3-6. create, authorize (async), capture (async)
        UUID payment = createPayment(m, "cust_happy", 10_000);
        assertEquals("CREATED", payment(m, payment).get("status").asText());
        Resp auth = send("POST", "/api/v1/payments/" + payment + "/authorize", m.apiKey(), null);
        assertEquals(202, auth.status(), "authorize is asynchronous: 202, never a premature final result");
        assertEquals("PROCESSING", auth.body().get("status").asText());
        awaitPayment(m, payment, "AUTHORIZED");
        UUID captureId = capture(m, payment, null);
        JsonNode captured = awaitPayment(m, payment, "CAPTURED");
        assertEquals(10_000, captured.get("capturedMinor").asLong());

        // 7. exactly one logical capture
        assertEquals(1, count("SELECT count(*) FROM captures WHERE payment_id = ?", payment));
        assertEquals("SUCCEEDED", jdbc.queryForObject("SELECT status FROM captures WHERE id = ?", String.class, captureId));
        assertEquals(1, count("SELECT count(*) FROM provider_operations WHERE operation_id = ?", "cap-" + captureId), "one provider-side capture");
        assertTrue(count("SELECT count(*) FROM payment_attempts WHERE payment_id = ? AND status = 'SUCCEEDED'", payment) >= 2, "attempts recorded for diagnostics");

        // 8. balanced ledger journal
        UUID journal = jdbc.queryForObject("SELECT id FROM ledger_journals WHERE reference_type = 'CAPTURE' AND reference_id = ?", UUID.class, captureId);
        JsonNode j = expect(200, send("GET", "/api/v1/ledger/journals/" + journal, m.apiKey(), null)).body();
        long debits = 0;
        long credits = 0;
        for (JsonNode e : j.get("entries")) {
            debits += e.get("debitMinor").asLong();
            credits += e.get("creditMinor").asLong();
        }
        assertEquals(10_000, debits);
        assertEquals(10_000, credits);
        assertEquals(10_000, expect(200, send("GET", "/api/v1/ledger/balance", m.apiKey(), null)).body().get("merchantPayableMinor").asLong());

        // 9. events published through the outbox to Kafka
        await("outbox drained", Duration.ofSeconds(20), () -> count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND status = 'PENDING'", payment), n -> n == 0);
        List<String> types = readTopic(events()).stream().filter(r -> payment.toString().equals(r.key())).map(r -> r.value()).toList();
        assertTrue(types.stream().anyMatch(v -> v.contains("\"payment.captured\"")), "payment.captured event on Kafka: " + types.size() + " events for this payment");
        assertTrue(types.stream().anyMatch(v -> v.contains("\"payment.authorized\"")));

        // 10. webhooks delivered, signed, de-duplicable
        await("webhooks delivered", Duration.ofSeconds(30), () -> count("SELECT count(*) FROM webhook_deliveries WHERE endpoint_id = ? AND status = 'DELIVERED'", endpointId), n -> n >= 3);
        JsonNode receipts = send("GET", "/internal/test-receiver/" + endpointId + "/receipts", null, null).body();
        assertEquals(receipts.get("total").asInt(), receipts.get("validSignatures").asInt(), "every delivery carried a valid HMAC signature");

        // 11-12. partial refund -> ledger posting
        Resp r = refund(m, payment, 4_000, "refund-key-1");
        assertEquals(202, r.status());
        UUID refundId = UUID.fromString(r.body().get("id").asText());
        assertEquals("SUCCEEDED", awaitRefund(refundId, "SUCCEEDED"));
        JsonNode afterRefund = payment(m, payment);
        assertEquals(4_000, afterRefund.get("refundedMinor").asLong());
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'REFUND' AND reference_id = ?", refundId));
        assertEquals(6_000, expect(200, send("GET", "/api/v1/ledger/balance", m.apiKey(), null)).body().get("merchantPayableMinor").asLong(), "10,000 captured - 4,000 refunded");
        assertEquals(1, expect(200, send("GET", "/api/v1/payments/" + payment + "/refunds", m.apiKey(), null)).body().size());
        assertEquals(2, expect(200, send("GET", "/api/v1/transactions", m.apiKey(), null)).body().get("items").size(), "one capture + one refund");

        // 13-14. reconciliation reports nothing for the successful flow
        UUID runId = UUID.fromString(expect(201, send("POST", "/api/v1/reconciliation/runs", ADMIN_KEY, null)).body().get("id").asText());
        assertEquals(0, count("SELECT count(*) FROM reconciliation_discrepancies WHERE run_id = ? AND payment_id = ?", runId, payment), "no discrepancies for this payment");
        assertEquals(0, count("SELECT count(*) FROM reconciliation_discrepancies WHERE run_id = ? AND type IN ('UNBALANCED_JOURNAL','DUPLICATE_JOURNAL')", runId));
    }

    @Test
    void declinedAuthorizationFailsThePayment() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_decline", 500);
        authorize(m, p);
        JsonNode failed = awaitPayment(m, p, "FAILED");
        assertEquals("card_declined", failed.get("failureCode").asText());
        expect(409, send("POST", "/api/v1/payments/" + p + "/capture", m.apiKey(), Map.of(), "Idempotency-Key", UUID.randomUUID().toString()));
        assertEquals(0, count("SELECT count(*) FROM ledger_journals WHERE merchant_id = ?", m.id()));
    }

    @Test
    void cancelWorksOnlyBeforeCapture() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_cancel", 700);
        expect(409, send("POST", "/api/v1/payments/" + p + "/cancel", m.apiKey(), null));   // CREATED cannot be canceled
        authorize(m, p);
        awaitPayment(m, p, "AUTHORIZED");
        assertEquals("CANCELED", expect(200, send("POST", "/api/v1/payments/" + p + "/cancel", m.apiKey(), null)).body().get("status").asText());
        expect(409, send("POST", "/api/v1/payments/" + p + "/capture", m.apiKey(), Map.of(), "Idempotency-Key", UUID.randomUUID().toString()));
        UUID captured = capturedPayment(m, "cust_cancel2", 700);
        expect(409, send("POST", "/api/v1/payments/" + captured + "/cancel", m.apiKey(), null));
    }

    @Test
    void partialCaptureAndCaptureLimits() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_partial", 1000);
        authorize(m, p);
        awaitPayment(m, p, "AUTHORIZED");
        expect(422, send("POST", "/api/v1/payments/" + p + "/capture", m.apiKey(), Map.of("amountMinor", 1001), "Idempotency-Key", UUID.randomUUID().toString()));
        capture(m, p, 600L);
        JsonNode done = awaitPayment(m, p, "CAPTURED");
        assertEquals(600, done.get("capturedMinor").asLong());
        assertEquals(600, expect(200, send("GET", "/api/v1/ledger/balance", m.apiKey(), null)).body().get("merchantPayableMinor").asLong());
    }

    @Test
    void listingFilteringSortingAndPagination() {
        Merchant m = newMerchant();
        for (long a : new long[] {300, 100, 200}) {
            createPayment(m, "cust_list", a);
        }
        createPayment(m, "cust_other", 999);
        JsonNode page = expect(200, send("GET", "/api/v1/payments?customerId=cust_list&sort=amountMinor,asc&size=2", m.apiKey(), null)).body();
        assertEquals(2, page.get("items").size());
        assertEquals(100, page.get("items").get(0).get("amountMinor").asLong());
        assertEquals(3, page.get("totalElements").asInt());
        assertEquals(4, expect(200, send("GET", "/api/v1/payments?status=CREATED", m.apiKey(), null)).body().get("totalElements").asInt());
        expect(400, send("GET", "/api/v1/payments?size=1000", m.apiKey(), null));
        expect(400, send("GET", "/api/v1/payments?sort=password_hash", m.apiKey(), null));
        expect(400, send("GET", "/api/v1/payments?status=BOGUS", m.apiKey(), null));
    }
}
