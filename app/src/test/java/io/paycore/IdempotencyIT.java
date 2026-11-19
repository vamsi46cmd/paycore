package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

class IdempotencyIT extends TestSupport {
    @Autowired Idempotency idempotency;

    private Resp create(Merchant m, String key, String rawJson) {
        return send("POST", "/api/v1/payments", m.apiKey(), rawJson, "Idempotency-Key", key);
    }

    @Test
    void sameKeySamePayloadReturnsTheSameLogicalResult() {
        Merchant m = newMerchant();
        String key = UUID.randomUUID().toString();
        Resp first = expect(201, create(m, key, "{\"amountMinor\":1500,\"currency\":\"USD\",\"customerId\":\"c1\"}"));
        // identical payload with the JSON keys in a different order is still "the same request"
        Resp second = expect(201, create(m, key, "{\"customerId\":\"c1\",\"currency\":\"USD\",\"amountMinor\":1500}"));
        assertEquals(first.body().get("id").asText(), second.body().get("id").asText());
        assertEquals("true", second.headers().firstValue("Idempotent-Replayed").orElse(null));
        assertTrue(first.headers().firstValue("Idempotent-Replayed").isEmpty());
        assertEquals(1, count("SELECT count(*) FROM payments WHERE merchant_id = ?", m.id()));
    }

    @Test
    void sameKeyDifferentPayloadIsAConflict() {
        Merchant m = newMerchant();
        String key = UUID.randomUUID().toString();
        expect(201, create(m, key, "{\"amountMinor\":1500,\"currency\":\"USD\",\"customerId\":\"c1\"}"));
        Resp r = expect(409, create(m, key, "{\"amountMinor\":1501,\"currency\":\"USD\",\"customerId\":\"c1\"}"));
        assertEquals("idempotency_key_conflict", r.body().get("code").asText());
        expect(409, create(m, key, "{\"amountMinor\":1500,\"currency\":\"USD\",\"customerId\":\"c2\"}"));
        assertEquals(1, count("SELECT count(*) FROM payments WHERE merchant_id = ?", m.id()));
    }

    @Test
    void keysAreScopedPerMerchantAndOperation() {
        Merchant a = newMerchant();
        Merchant b = newMerchant();
        String key = "shared-key-" + UUID.randomUUID();
        String body = "{\"amountMinor\":100,\"currency\":\"USD\",\"customerId\":\"c\"}";
        JsonNode pa = expect(201, create(a, key, body)).body();
        JsonNode pb = expect(201, create(b, key, body)).body();
        assertTrue(!pa.get("id").asText().equals(pb.get("id").asText()), "another merchant's identical key is a different operation");
    }

    @Test
    void concurrentIdenticalRequestsCreateExactlyOnePayment() throws Exception {
        Merchant m = newMerchant();
        String key = UUID.randomUUID().toString();
        String body = "{\"amountMinor\":2500,\"currency\":\"USD\",\"customerId\":\"c_conc\"}";
        List<Resp> results = runConcurrently(16, i -> () -> create(m, key, body));
        Set<String> ids = new HashSet<>();
        for (Resp r : results) {
            assertEquals(201, r.status(), "every identical concurrent request gets the same success: " + r.body());
            ids.add(r.body().get("id").asText());
        }
        assertEquals(1, ids.size(), "all 16 responses describe ONE logical payment");
        assertEquals(1, count("SELECT count(*) FROM payments WHERE merchant_id = ?", m.id()));
        assertEquals(1, count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?::uuid AND event_type = 'payment.created'", ids.iterator().next()));
    }

    @Test
    void concurrentConflictingRequestsLetExactlyOneWin() throws Exception {
        Merchant m = newMerchant();
        String key = UUID.randomUUID().toString();
        List<Resp> results = runConcurrently(16, i -> () -> create(m, key, "{\"amountMinor\":" + (i % 2 == 0 ? 100 : 200) + ",\"currency\":\"USD\",\"customerId\":\"c\"}"));
        Set<String> createdIds = new HashSet<>();
        int conflicts = 0;
        for (Resp r : results) {
            assertTrue(r.status() == 201 || r.status() == 409, "unexpected " + r.status() + " " + r.body());
            if (r.status() == 201) createdIds.add(r.body().get("id").asText());
            else conflicts++;
        }
        assertEquals(1, createdIds.size(), "only one payment is ever created for the key");
        assertTrue(conflicts >= 1, "the other payload must be refused");
        assertEquals(1, count("SELECT count(*) FROM payments WHERE merchant_id = ?", m.id()));
    }

    @Test
    void aFailedTransactionLeavesNoKeyAndTheRetrySucceeds() {
        Merchant m = newMerchant();
        String key = UUID.randomUUID().toString();
        // simulate the API (or its database transaction) dying after partially applying the effect
        assertThrows(IllegalStateException.class, () -> idempotency.execute(m.id(), "payments.create", key, Map.of("x", 1), () -> {
            jdbc.update("INSERT INTO payments (id, merchant_id, customer_id, amount_minor, currency, status, idempotency_key) VALUES (?,?, 'c', 100, 'USD', 'CREATED', ?)", UUID.randomUUID(), m.id(), key);
            throw new IllegalStateException("crash mid-transaction");
        }));
        assertEquals(0, count("SELECT count(*) FROM payments WHERE merchant_id = ?", m.id()), "partial effects rolled back");
        assertEquals(0, count("SELECT count(*) FROM idempotency_keys WHERE merchant_id = ? AND idem_key = ?", m.id(), key), "no key without its effect");
        expect(201, create(m, key, "{\"amountMinor\":100,\"currency\":\"USD\",\"customerId\":\"c\"}"));
        assertEquals(1, count("SELECT count(*) FROM payments WHERE merchant_id = ?", m.id()));
    }

    @Test
    void theKeyAndResponseAreDurableInThePostgresNotMemory() {
        Merchant m = newMerchant();
        String key = UUID.randomUUID().toString();
        JsonNode p = expect(201, create(m, key, "{\"amountMinor\":777,\"currency\":\"USD\",\"customerId\":\"c\"}")).body();
        Map<String, Object> row = jdbc.queryForMap("SELECT response_status, response_body, fingerprint, expires_at > now() AS live FROM idempotency_keys WHERE merchant_id = ? AND idem_key = ?", m.id(), key);
        assertEquals(201, row.get("response_status"));
        assertTrue(((String) row.get("response_body")).contains(p.get("id").asText()), "replayable response is stored, so a restarted API can replay it");
        assertEquals(64, ((String) row.get("fingerprint")).length());
        assertEquals(true, row.get("live"));
    }

    @Test
    void afterRetentionExpiryAKeyCannotBeSilentlyReused() {
        Merchant m = newMerchant();
        String key = UUID.randomUUID().toString();
        String body = "{\"amountMinor\":100,\"currency\":\"USD\",\"customerId\":\"c\"}";
        expect(201, create(m, key, body));
        jdbc.update("UPDATE idempotency_keys SET expires_at = now() - interval '1 minute' WHERE merchant_id = ? AND idem_key = ?", m.id(), key);
        // the stored response expired, but the payment still carries the key: a second payment is refused rather than created
        Resp r = expect(409, create(m, key, body));
        assertEquals("idempotency_key_reused", r.body().get("code").asText());
        assertEquals(1, count("SELECT count(*) FROM payments WHERE merchant_id = ?", m.id()));
    }

    @Test
    void headerAndInputValidation() {
        Merchant m = newMerchant();
        expect(400, send("POST", "/api/v1/payments", m.apiKey(), Map.of("amountMinor", 100, "currency", "USD", "customerId", "c")));   // no Idempotency-Key
        String k = UUID.randomUUID().toString();
        expect(400, create(m, k, "{\"amountMinor\":-5,\"currency\":\"USD\",\"customerId\":\"c\"}"));
        expect(400, create(m, k, "{\"amountMinor\":0,\"currency\":\"USD\",\"customerId\":\"c\"}"));
        expect(400, create(m, k, "{\"amountMinor\":100,\"currency\":\"EUR\",\"customerId\":\"c\"}"));
        expect(400, create(m, k, "{\"amountMinor\":100,\"currency\":\"USD\",\"customerId\":\"bad id!\"}"));
        expect(400, create(m, k, "{\"amountMinor\":99999999999,\"currency\":\"USD\",\"customerId\":\"c\"}"));
        expect(400, create(m, k, "{\"amountMinor\":12.5,\"currency\":\"USD\",\"customerId\":\"c\"}"));   // decimals are rejected, not truncated
        expect(400, create(m, k, "{\"amountMinor\":\"100\",\"currency\":\"USD\",\"customerId\":\"c\"}"));
        expect(400, create(m, k, "not json"));
        expect(400, create(m, "x".repeat(101), "{\"amountMinor\":100,\"currency\":\"USD\",\"customerId\":\"c\"}"));
        assertEquals(0, count("SELECT count(*) FROM payments WHERE merchant_id = ?", m.id()));
    }

    @Test
    void captureIsIdempotentAndOnlyOneCaptureCanBeLive() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_capidem", 1000);
        authorize(m, p);
        awaitPayment(m, p, "AUTHORIZED");
        String key = UUID.randomUUID().toString();
        Resp first = expect(202, send("POST", "/api/v1/payments/" + p + "/capture", m.apiKey(), Map.of(), "Idempotency-Key", key));
        Resp replay = expect(202, send("POST", "/api/v1/payments/" + p + "/capture", m.apiKey(), Map.of(), "Idempotency-Key", key));
        assertEquals(first.body().get("captureId").asText(), replay.body().get("captureId").asText());
        assertEquals("true", replay.headers().firstValue("Idempotent-Replayed").orElse(null));
        expect(409, send("POST", "/api/v1/payments/" + p + "/capture", m.apiKey(), Map.of("amountMinor", 500), "Idempotency-Key", key));   // same key, different body
        Resp other = send("POST", "/api/v1/payments/" + p + "/capture", m.apiKey(), Map.of(), "Idempotency-Key", UUID.randomUUID().toString());
        assertTrue(other.status() == 409, "a second capture with a new key is refused while one is live: " + other.body());
        awaitPayment(m, p, "CAPTURED");
        assertEquals(1, count("SELECT count(*) FROM captures WHERE payment_id = ?", p));
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'CAPTURE' AND merchant_id = ?", m.id()));
    }

    @Test
    void concurrentCapturesWithDifferentKeysProduceExactlyOneCapture() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_conccap", 4200);
        authorize(m, p);
        awaitPayment(m, p, "AUTHORIZED");
        List<Resp> results = runConcurrently(12, i -> () -> send("POST", "/api/v1/payments/" + p + "/capture", m.apiKey(), Map.of(), "Idempotency-Key", UUID.randomUUID().toString()));
        long accepted = results.stream().filter(r -> r.status() == 202).count();
        results.stream().filter(r -> r.status() != 202).forEach(r -> assertEquals(409, r.status(), r.body().toString()));
        assertEquals(1, accepted, "exactly one of 12 concurrent captures may win");
        awaitPayment(m, p, "CAPTURED");
        assertEquals(1, count("SELECT count(*) FROM captures WHERE payment_id = ? AND status IN ('PENDING','SUCCEEDED')", p));
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'CAPTURE' AND merchant_id = ?", m.id()));
        assertEquals(4200, expect(200, send("GET", "/api/v1/ledger/balance", m.apiKey(), null)).body().get("merchantPayableMinor").asLong(), "captured exactly once, not 12 times");
    }
}
