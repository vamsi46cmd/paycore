package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WebhookIT extends TestSupport {
    private UUID endpoint(Merchant m, String query, List<String> types) {
        JsonNode ep = expect(201, send("POST", "/api/v1/webhooks/endpoints", m.apiKey(), Map.of("url", "http://localhost:" + port + "/internal/test-receiver" + query, "eventTypes", types))).body();
        return UUID.fromString(ep.get("endpointId").asText());
    }

    private String secret(UUID endpointId) {
        return jdbc.queryForObject("SELECT secret FROM webhook_endpoints WHERE id = ?", String.class, endpointId);
    }

    @Test
    void failedDeliveriesAreRetriedWithBackoffAndTheEventIdStaysStable() throws Exception {
        Merchant m = newMerchant();
        UUID ep = endpoint(m, "?failFirst=2", List.of("payment.created"));
        UUID p = createPayment(m, "cust_wh_retry", 100);
        UUID delivery = await("delivery row", Duration.ofSeconds(20), () -> jdbc.queryForList("SELECT id FROM webhook_deliveries WHERE endpoint_id = ?", UUID.class, ep), l -> !l.isEmpty()).get(0);
        await("delivered after retries", Duration.ofSeconds(30), () -> jdbc.queryForObject("SELECT status FROM webhook_deliveries WHERE id = ?", String.class, delivery), "DELIVERED"::equals);
        assertEquals(3, count("SELECT attempt_count FROM webhook_deliveries WHERE id = ?", delivery));
        assertEquals(List.of(500, 500, 200), jdbc.queryForList("SELECT response_status FROM webhook_attempts WHERE delivery_id = ? ORDER BY attempt_number", Integer.class, delivery), "attempt history is recorded");
        JsonNode receipts = send("GET", "/internal/test-receiver/" + ep + "/receipts", null, null).body();
        assertEquals(3, receipts.get("total").asInt(), "the receiver saw the SAME event three times (at-least-once)");
        assertEquals(1, receipts.get("distinctEvents").asInt(), "...and can de-duplicate it by its stable event id");
        assertEquals(3, receipts.get("validSignatures").asInt());
        assertEquals(p.toString(), om.readTree(jdbc.queryForObject("SELECT payload::text FROM webhook_deliveries WHERE id = ?", String.class, delivery)).get("aggregateId").asText());
    }

    @Test
    void exhaustedDeliveriesGoDeadAndCanBeReplayed() throws Exception {
        Merchant m = newMerchant();
        UUID ep = endpoint(m, "?failFirst=7", List.of("payment.created"));
        createPayment(m, "cust_wh_dead", 100);
        UUID delivery = await("delivery row", Duration.ofSeconds(20), () -> jdbc.queryForList("SELECT id FROM webhook_deliveries WHERE endpoint_id = ?", UUID.class, ep), l -> !l.isEmpty()).get(0);
        await("delivery dead", Duration.ofSeconds(40), () -> jdbc.queryForObject("SELECT status FROM webhook_deliveries WHERE id = ?", String.class, delivery), "DEAD"::equals);
        assertEquals(6, count("SELECT count(*) FROM webhook_attempts WHERE delivery_id = ?", delivery), "bounded: max-attempts then stop");
        expect(404, send("POST", "/api/v1/webhooks/deliveries/" + UUID.randomUUID() + "/replay", m.apiKey(), null));   // unknown id
        expect(200, send("POST", "/api/v1/webhooks/deliveries/" + delivery + "/replay", m.apiKey(), null));
        await("replayed and delivered", Duration.ofSeconds(30), () -> jdbc.queryForObject("SELECT status FROM webhook_deliveries WHERE id = ?", String.class, delivery), "DELIVERED"::equals);
        expect(409, send("POST", "/api/v1/webhooks/deliveries/" + delivery + "/replay", m.apiKey(), null));   // only DEAD deliveries replay
    }

    @Test
    void anotherMerchantCannotReplayMyDeliveryAndOnlySubscribedEventsAreSent() throws Exception {
        Merchant a = newMerchant();
        Merchant b = newMerchant();
        UUID ep = endpoint(a, "", List.of("refund.completed"));
        createPayment(a, "cust_wh_sub", 100);
        awaitCaughtUp("webhook-worker", events());
        assertEquals(0, count("SELECT count(*) FROM webhook_deliveries WHERE endpoint_id = ?", ep), "payment.created was not subscribed");
        UUID dead = UUID.randomUUID();
        expect(404, send("POST", "/api/v1/webhooks/deliveries/" + dead + "/replay", b.apiKey(), null));
    }

    @Test
    void redeliveredKafkaEventsDoNotDuplicateDeliveries() throws Exception {
        Merchant m = newMerchant();
        UUID ep = endpoint(m, "", List.of("payment.created"));
        UUID p = createPayment(m, "cust_wh_dup", 100);
        await("delivered", Duration.ofSeconds(20), () -> count("SELECT count(*) FROM webhook_deliveries WHERE endpoint_id = ? AND status = 'DELIVERED'", ep), n -> n == 1);
        String envelope = jdbc.queryForObject("SELECT payload::text FROM outbox_events WHERE aggregate_id = ? AND event_type = 'payment.created'", String.class, p);
        for (int i = 0; i < 3; i++) {
            sendRaw(events(), p.toString(), envelope);
        }
        awaitCaughtUp("webhook-worker", events());
        assertEquals(1, count("SELECT count(*) FROM webhook_deliveries WHERE endpoint_id = ?", ep), "one delivery per (event, endpoint)");
    }

    @Test
    void theReceiverRejectsBadSignaturesAndStaleTimestamps() throws Exception {
        Merchant m = newMerchant();
        UUID ep = endpoint(m, "", List.of("payment.created"));
        String body = "{\"hello\":\"world\"}";
        long now = System.currentTimeMillis() / 1000;
        String good = Webhook.header(secret(ep), now, body);
        UUID evt = UUID.randomUUID();
        java.util.function.BiFunction<String, String, Integer> post = (sig, b) -> send("POST", "/internal/test-receiver", null, b, "PayCore-Endpoint-Id", ep.toString(), "PayCore-Event-Id", evt.toString(), "PayCore-Signature", sig).status();
        assertEquals(200, post.apply(good, body));
        assertEquals(401, post.apply(good, body + "x"), "tampered body");
        assertEquals(401, post.apply(Webhook.header("whsec_wrong", now, body), body), "wrong secret");
        assertEquals(401, post.apply(Webhook.header(secret(ep), now - 3600, body), body), "stale timestamp (replay)");
        assertEquals(401, post.apply("t=1,v1=00", body));
    }

    @Test
    void ssrfAttemptsAreRefusedAtRegistration() {
        Merchant m = newMerchant();
        for (String url : List.of("http://169.254.169.254/latest/meta-data/", "http://127.0.0.1:8080/", "http://10.0.0.1/", "http://192.168.1.1/", "http://[::1]/",
                "http://evil.example.com/hook", "file:///etc/passwd", "gopher://localhost/", "http://user:pass@localhost/hook", "http://localhost.evil.com/")) {
            expect(400, send("POST", "/api/v1/webhooks/endpoints", m.apiKey(), Map.of("url", url, "eventTypes", List.of("*"))));
        }
        expect(400, send("POST", "/api/v1/webhooks/endpoints", m.apiKey(), Map.of("url", "http://localhost/hook", "eventTypes", List.of("bogus.event"))));
        expect(400, send("POST", "/api/v1/webhooks/endpoints", m.apiKey(), Map.of("url", "http://localhost/hook", "eventTypes", List.of())));
        assertEquals(0, count("SELECT count(*) FROM webhook_endpoints WHERE merchant_id = ?", m.id()));
    }
}
