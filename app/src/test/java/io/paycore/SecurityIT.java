package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SecurityIT extends TestSupport {
    @Test
    void everyMerchantEndpointRequiresAValidApiKey() {
        for (String path : List.of("/api/v1/payments", "/api/v1/transactions", "/api/v1/ledger/balance", "/api/v1/webhooks/endpoints", "/api/v1/payments/" + UUID.randomUUID())) {
            expect(401, send("GET", path, null, null));
            expect(401, send("GET", path, "pk_not_a_real_key", null));
        }
        expect(401, send("POST", "/api/v1/payments", null, Map.of("amountMinor", 1, "currency", "USD", "customerId", "c"), "Idempotency-Key", "k"));
    }

    @Test
    void merchantsCannotSeeOrTouchEachOthersData() throws Exception {
        Merchant a = newMerchant();
        Merchant b = newMerchant();
        UUID p = capturedPayment(a, "cust_iso", 1000);
        UUID bp = createPayment(b, "cust_iso_b", 500);
        UUID journal = jdbc.queryForObject("SELECT id FROM ledger_journals WHERE merchant_id = ?", UUID.class, a.id());
        expect(200, send("GET", "/api/v1/payments/" + p, a.apiKey(), null));
        // guessing a real id from another tenant is indistinguishable from an unknown id: 404, never data
        expect(404, send("GET", "/api/v1/payments/" + p, b.apiKey(), null));
        expect(404, send("POST", "/api/v1/payments/" + p + "/authorize", b.apiKey(), null));
        expect(404, send("POST", "/api/v1/payments/" + p + "/cancel", b.apiKey(), null));
        expect(404, send("GET", "/api/v1/payments/" + p + "/refunds", b.apiKey(), null));
        assertEquals(404, refund(b, p, 100, "k1").status());
        expect(404, send("POST", "/api/v1/payments/" + p + "/capture", b.apiKey(), Map.of(), "Idempotency-Key", "k2"));
        expect(404, send("GET", "/api/v1/ledger/journals/" + journal, b.apiKey(), null));
        expect(200, send("GET", "/api/v1/ledger/journals/" + journal, a.apiKey(), null));
        assertEquals(1, expect(200, send("GET", "/api/v1/payments", b.apiKey(), null)).body().get("totalElements").asInt(), "only my own payment is listed");
        assertEquals(0, expect(200, send("GET", "/api/v1/transactions", b.apiKey(), null)).body().get("items").size());
        assertEquals(0, expect(200, send("GET", "/api/v1/ledger/balance", b.apiKey(), null)).body().get("merchantPayableMinor").asLong());
        assertEquals("CREATED", payment(b, bp).get("status").asText());
        assertEquals("CAPTURED", payment(a, p).get("status").asText(), "the other merchant's attempts changed nothing");
    }

    @Test
    void operatorEndpointsNeedTheAdminKeyAndMerchantKeysAreNotAdminKeys() {
        Merchant m = newMerchant();
        expect(401, send("POST", "/api/v1/merchants", null, Map.of("name", "x")));
        expect(403, send("POST", "/api/v1/merchants", m.apiKey(), Map.of("name", "x")));
        expect(403, send("POST", "/api/v1/reconciliation/runs", m.apiKey(), null));
        expect(403, send("GET", "/api/v1/reconciliation/runs", m.apiKey(), null));
        expect(401, send("POST", "/api/v1/reconciliation/runs", "wrong-admin-key-wrong-admin-key", null));
        expect(200, send("GET", "/api/v1/reconciliation/runs", ADMIN_KEY, null));
        expect(403, send("GET", "/api/v1/payments", ADMIN_KEY, null));   // admin is not a merchant
    }

    @Test
    void apiKeysAreStoredOnlyAsHashes() {
        Merchant m = newMerchant();
        String stored = jdbc.queryForObject("SELECT api_key_hash FROM merchants WHERE id = ?", String.class, m.id());
        assertNotEquals(m.apiKey(), stored);
        assertEquals(SecurityConfig.sha256(m.apiKey()), stored);
        assertFalse(stored.contains(m.apiKey()));
    }

    @Test
    void inputLimitsAndSafeErrors() {
        Merchant m = newMerchant();
        expect(413, send("POST", "/api/v1/payments", m.apiKey(), "{\"customerId\":\"" + "x".repeat(70_000) + "\"}", "Idempotency-Key", "big"));
        Resp bad = expect(400, send("GET", "/api/v1/payments/not-a-uuid", m.apiKey(), null));
        String text = bad.body().toString();
        assertFalse(text.contains("Exception") || text.contains("at io.paycore") || text.contains("SELECT"), "no internals in errors: " + text);
        expect(404, send("GET", "/api/v1/payments/" + UUID.randomUUID(), m.apiKey(), null));
        assertTrue(send("GET", "/actuator/prometheus", null, null).status() != 200, "metrics are not served on the public port");
        // SQL injection attempts are inert: all values are bound parameters, sort/status are whitelisted
        expect(400, send("GET", "/api/v1/payments?status=CREATED%27%20OR%20%271%27=%271", m.apiKey(), null));
        expect(400, send("GET", "/api/v1/payments?sort=createdAt;DROP%20TABLE%20payments", m.apiKey(), null));
        assertEquals(0, expect(200, send("GET", "/api/v1/payments?customerId=%27%20OR%201=1--", m.apiKey(), null)).body().get("totalElements").asInt());
        assertTrue(count("SELECT count(*) FROM payments") >= 0);
    }

    @Test
    void everyStateChangeIsAudited() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_audit", 300);
        List<String> actions = jdbc.queryForList("SELECT action FROM audit_log WHERE merchant_id = ? ORDER BY created_at", String.class, m.id());
        assertTrue(actions.containsAll(List.of("payment.created", "payment.authorize.requested", "payment.capture.requested")), actions.toString());
        assertTrue(count("SELECT count(*) FROM audit_log WHERE merchant_id = ? AND request_id IS NOT NULL", m.id()) >= 3, "audit rows carry the correlation id");
    }

    @Test
    void correlationIdIsEchoedAndPropagatesIntoEvents() throws Exception {
        Merchant m = newMerchant();
        String corr = "corr-" + UUID.randomUUID();
        Resp r = send("POST", "/api/v1/payments", m.apiKey(), Map.of("amountMinor", 100, "currency", "USD", "customerId", "cust_corr"), "Idempotency-Key", UUID.randomUUID().toString(), "X-Request-Id", corr);
        assertEquals(201, r.status());
        assertEquals(corr, r.headers().firstValue("X-Request-Id").orElse(null));
        UUID p = UUID.fromString(r.body().get("id").asText());
        assertEquals(corr, om.readTree(jdbc.queryForObject("SELECT payload::text FROM outbox_events WHERE aggregate_id = ? AND event_type = 'payment.created'", String.class, p)).get("correlationId").asText());
    }
}
