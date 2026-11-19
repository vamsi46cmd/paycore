package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class WorkerResilienceIT extends TestSupport {
    @Autowired PaymentWorker worker;

    private String commandJson(UUID paymentId, String type) {
        return jdbc.queryForObject("SELECT payload::text FROM outbox_events WHERE aggregate_id = ? AND event_type = ? ORDER BY created_at LIMIT 1", String.class, paymentId, type);
    }

    private long payable(Merchant m) {
        return expect(200, send("GET", "/api/v1/ledger/balance", m.apiKey(), null)).body().get("merchantPayableMinor").asLong();
    }

    @Test
    void theSameCaptureCommandDeliveredManyTimesHasOneFinancialEffect() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_redeliver", 5000);
        String original = commandJson(p, "payment.capture.requested");
        for (int i = 0; i < 4; i++) {
            sendRaw(commands(), p.toString(), original);   // Kafka redelivery of the exact same event
        }
        awaitCaughtUp("payment-worker", commands());
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'CAPTURE' AND merchant_id = ?", m.id()), "replayed events post no extra journal");
        assertEquals(2, count("SELECT count(*) FROM ledger_entries e JOIN ledger_journals j ON j.id = e.journal_id WHERE j.merchant_id = ?", m.id()));
        assertEquals(5000, payable(m));
        assertEquals(1, count("SELECT count(*) FROM captures WHERE payment_id = ? AND status = 'SUCCEEDED'", p));
        assertEquals("CAPTURED", payment(m, p).get("status").asText());
    }

    @Test
    void aStaleEventWithAFreshIdIsStillHarmless() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_stale", 1200);
        JsonNode env = om.readTree(commandJson(p, "payment.capture.requested"));
        ((com.fasterxml.jackson.databind.node.ObjectNode) env).put("eventId", UUID.randomUUID().toString());   // a different event id, same capture
        sendRaw(commands(), p.toString(), env.toString());
        // direct handler invocation too (same JVM): the state guard and the unique ledger reference both hold
        worker.handle(Events.parse(om, env.toString()));
        awaitCaughtUp("payment-worker", commands());
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'CAPTURE' AND merchant_id = ?", m.id()));
        assertEquals(1200, payable(m));
    }

    @Test
    void duplicateAuthorizeCommandsDoNotAuthorizeTwice() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_dupauth", 900);
        authorize(m, p);
        awaitPayment(m, p, "AUTHORIZED");
        String original = commandJson(p, "payment.authorize.requested");
        for (int i = 0; i < 3; i++) {
            sendRaw(commands(), p.toString(), original);
        }
        awaitCaughtUp("payment-worker", commands());
        assertEquals(1, count("SELECT count(*) FROM authorizations WHERE payment_id = ?", p));
        assertEquals(1, count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'payment.authorized'", p));
        assertEquals(1, count("SELECT count(*) FROM provider_operations WHERE operation_id = ?", "auth-" + p));
    }

    @Test
    void providerTimeoutAfterTheProviderRecordedSuccess() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_timeout_after_success", 3000);
        UUID captureId = jdbc.queryForObject("SELECT id FROM captures WHERE payment_id = ?", UUID.class, p);
        assertEquals(1, count("SELECT calls FROM provider_call_log WHERE operation_id = ?", "cap-" + captureId), "no second provider call after the ambiguous timeout");
        assertEquals(1, count("SELECT count(*) FROM provider_operations WHERE operation_id = ?", "cap-" + captureId), "exactly one provider-side capture");
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'CAPTURE' AND reference_id = ?", captureId));
        assertEquals(3000, payable(m));
    }

    @Test
    void providerTimeoutThatRecordedNothingIsRetriedWithTheSameOperationId() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_timeout_no_effect", 800);
        UUID captureId = jdbc.queryForObject("SELECT id FROM captures WHERE payment_id = ?", UUID.class, p);
        assertEquals(2, count("SELECT calls FROM provider_call_log WHERE operation_id = ?", "cap-" + captureId), "timed out once, retried once");
        assertEquals(1, count("SELECT count(*) FROM provider_operations WHERE operation_id = ?", "cap-" + captureId));
        assertEquals(1, count("SELECT count(*) FROM payment_attempts WHERE payment_id = ? AND operation = 'CAPTURE' AND status = 'RETRYABLE_ERROR' AND error_code = 'timeout_unknown'", p), "the failed attempt is traceable");
        assertEquals(1, count("SELECT count(*) FROM payment_attempts WHERE payment_id = ? AND operation = 'CAPTURE' AND status = 'SUCCEEDED'", p));
        assertEquals(800, payable(m));
    }

    @Test
    void temporaryProviderUnavailabilityIsRetriedWithBackoff() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_unavailable", 600);
        authorize(m, p);
        awaitPayment(m, p, "AUTHORIZED");
        assertEquals(3, count("SELECT calls FROM provider_call_log WHERE operation_id = ?", "auth-" + p), "two unavailable responses, then success");
        assertEquals(2, count("SELECT count(*) FROM payment_attempts WHERE payment_id = ? AND status = 'RETRYABLE_ERROR' AND error_code = 'provider_unavailable'", p));
        assertEquals(1, count("SELECT count(*) FROM authorizations WHERE payment_id = ?", p));
    }

    @Test
    void failedCaptureLeavesThePaymentAuthorizedAndPostsNothing() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_capture_fail", 700);
        authorize(m, p);
        awaitPayment(m, p, "AUTHORIZED");
        UUID c1 = capture(m, p, null);
        await("capture failed", Duration.ofSeconds(30), () -> jdbc.queryForObject("SELECT status FROM captures WHERE id = ?", String.class, c1), "FAILED"::equals);
        assertEquals("AUTHORIZED", payment(m, p).get("status").asText());
        assertEquals("capture_declined", jdbc.queryForObject("SELECT failure_code FROM captures WHERE id = ?", String.class, c1));
        assertEquals(0, count("SELECT count(*) FROM ledger_journals WHERE merchant_id = ?", m.id()));
        UUID c2 = capture(m, p, null);   // a failed capture may be retried with a new request
        await("second capture failed", Duration.ofSeconds(30), () -> jdbc.queryForObject("SELECT status FROM captures WHERE id = ?", String.class, c2), "FAILED"::equals);
        assertEquals(2, count("SELECT count(*) FROM captures WHERE payment_id = ? AND status = 'FAILED'", p));
        assertEquals(0, payable(m));
    }

    @Test
    void duplicateProviderCallbackHasNoExtraEffect() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_duplicate_callback", 2200);
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'CAPTURE' AND merchant_id = ?", m.id()));
        assertEquals(2200, payable(m));
        Resp r = refund(m, p, 200, "k-dc");
        awaitRefund(UUID.fromString(r.body().get("id").asText()), "SUCCEEDED");
        assertEquals(1, count("SELECT count(*) FROM ledger_journals WHERE reference_type = 'REFUND' AND merchant_id = ?", m.id()));
        assertEquals(2000, payable(m));
    }

    @Test
    void delayedProviderResponseStillCompletes() throws Exception {
        Merchant m = newMerchant();
        UUID p = capturedPayment(m, "cust_delayed", 100);
        assertEquals(100, payable(m));
        assertEquals("CAPTURED", payment(m, p).get("status").asText());
    }

    @Test
    void invalidMessagesGoStraightToTheDeadLetterTopic() throws Exception {
        String marker = UUID.randomUUID().toString();
        sendRaw(commands(), marker, "{ this is not json " + marker);
        sendRaw(commands(), marker, "{\"eventId\":\"" + UUID.randomUUID() + "\",\"eventType\":\"payment.capture.requested\",\"schemaVersion\":99,\"aggregateId\":\"" + UUID.randomUUID() + "\",\"payload\":{}}");
        UUID id = UUID.randomUUID();
        sendRaw(commands(), marker, "{\"eventId\":\"" + UUID.randomUUID() + "\",\"eventType\":\"payment.explode\",\"schemaVersion\":1,\"aggregateId\":\"" + id + "\",\"payload\":{\"x\":1}}");
        awaitCaughtUp("payment-worker", commands());
        List<String> dead = await("messages dead-lettered", Duration.ofSeconds(30),
                () -> readTopic(dlq()).stream().filter(r -> marker.equals(r.key())).map(r -> r.value()).toList(), l -> l.size() >= 3);
        assertEquals(3, dead.size(), "all three invalid messages ended in the DLQ, none were lost or retried forever");
    }
}
