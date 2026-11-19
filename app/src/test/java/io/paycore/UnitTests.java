package io.paycore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Unit tests: no database, Kafka or Redis needed. */
class UnitTests {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ---------------------------------------------------------------- money
    @Test
    void moneyArithmeticIsExactAndDetectsOverflow() {
        assertEquals(Money.usd(30), Money.usd(10).plus(Money.usd(20)));
        assertEquals(Money.usd(-5), Money.usd(10).minus(Money.usd(15)));
        assertThrows(ArithmeticException.class, () -> Money.usd(Long.MAX_VALUE).plus(Money.usd(1)));
        assertThrows(ArithmeticException.class, () -> Money.usd(Long.MIN_VALUE).minus(Money.usd(1)));
        // 0.10 + 0.20 is exactly 0.30 in minor units (the classic floating-point trap does not exist)
        assertEquals(Money.usd(30), Money.usd(10).plus(Money.usd(20)));
    }

    @Test
    void currencyAndAmountValidation() {
        assertThrows(IllegalArgumentException.class, () -> new Money(1, "EUR"));
        assertThrows(IllegalArgumentException.class, () -> Money.requireCurrency("usd"));
        assertThrows(IllegalArgumentException.class, () -> Money.requireValidAmount(0));
        assertThrows(IllegalArgumentException.class, () -> Money.requireValidAmount(-1));
        assertThrows(IllegalArgumentException.class, () -> Money.requireValidAmount(Money.MAX_MINOR + 1));
        assertEquals(1, Money.requireValidAmount(1));
        assertEquals(Money.MAX_MINOR, Money.requireValidAmount(Money.MAX_MINOR));
        assertEquals("123.45 USD", Money.usd(12345).toString());
        assertEquals("0.05 USD", Money.usd(5).toString());
        assertEquals("-1.00 USD", Money.usd(-100).toString());
    }

    // ---------------------------------------------------------------- state machine
    @Test
    void paymentStateMachineAllowsOnlyDocumentedTransitions() {
        Map<PaymentStatus, List<PaymentStatus>> allowed = Map.of(
            PaymentStatus.CREATED, List.of(PaymentStatus.PROCESSING),
            PaymentStatus.PROCESSING, List.of(PaymentStatus.AUTHORIZED, PaymentStatus.FAILED),
            PaymentStatus.AUTHORIZED, List.of(PaymentStatus.CAPTURED, PaymentStatus.CANCELED),
            PaymentStatus.CAPTURED, List.of(), PaymentStatus.FAILED, List.of(), PaymentStatus.CANCELED, List.of());
        for (PaymentStatus from : PaymentStatus.values()) {
            for (PaymentStatus to : PaymentStatus.values()) {
                assertEquals(allowed.get(from).contains(to), from.canTransitionTo(to), from + " -> " + to);
            }
        }
        assertTrue(PaymentStatus.CAPTURED.isTerminal());
        assertFalse(PaymentStatus.AUTHORIZED.isTerminal());
    }

    // ---------------------------------------------------------------- idempotency fingerprint
    @Test
    void canonicalFingerprintIgnoresKeyOrderButNotValues() throws Exception {
        String a = Idempotency.canonical(MAPPER.readTree("{\"b\":1,\"a\":{\"y\":2,\"x\":[1,2]}}"));
        String b = Idempotency.canonical(MAPPER.readTree("{\"a\":{\"x\":[1,2],\"y\":2},\"b\":1}"));
        String c = Idempotency.canonical(MAPPER.readTree("{\"a\":{\"x\":[2,1],\"y\":2},\"b\":1}"));
        assertEquals(a, b);
        assertFalse(a.equals(c), "array order and values matter");
        assertEquals(64, SecurityConfig.sha256(a).length());
    }

    // ---------------------------------------------------------------- webhook signing
    @Test
    void webhookSignatureVerifiesAndRejectsTampering() {
        String secret = "whsec_test";
        String body = "{\"eventId\":\"1\"}";
        long now = 1_700_000_000L;
        String header = Webhook.header(secret, now, body);
        assertTrue(Webhook.verify(secret, header, body, now + 10, 300));
        assertFalse(Webhook.verify(secret, header, body + " ", now, 300), "body tampering");
        assertFalse(Webhook.verify("other", header, body, now, 300), "wrong secret");
        assertFalse(Webhook.verify(secret, header, body, now + 301, 300), "stale timestamp (replay)");
        assertFalse(Webhook.verify(secret, null, body, now, 300));
        assertFalse(Webhook.verify(secret, "garbage", body, now, 300));
        assertFalse(Webhook.verify(secret, "t=abc,v1=00", body, now, 300));
    }

    // ---------------------------------------------------------------- SSRF policy
    private static UrlPolicy policy(boolean allowPublic, String... hosts) {
        Props p = new Props("x", new Props.Roles(true, true, true), new Props.Topics("", 6), "pc", new Props.Idempotency(Duration.ofHours(1)),
                new Props.Outbox(true, Duration.ofMillis(200), 100), new Props.Worker(3, 3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(5)),
                new Props.Provider(100, 2), new Props.Webhooks(List.of(hosts), 6, Duration.ofSeconds(1), 20, Duration.ofSeconds(3), true, allowPublic),
                new Props.Reconciliation(Duration.ofMinutes(5), false, Duration.ofMinutes(5)), new Props.RateLimit(100), new Props.TestHooks(0));
        return new UrlPolicy(p);
    }

    @Test
    void ssrfPolicyRejectsEverythingOutsideTheAllowlist() {
        UrlPolicy strict = policy(false, "payment-api", "localhost");
        assertDoesNotThrow(() -> strict.validate("http://payment-api:8080/hook"));
        assertDoesNotThrow(() -> strict.validate("https://localhost/hook"));
        for (String bad : List.of("http://169.254.169.254/latest/meta-data", "http://127.0.0.1/", "http://10.0.0.5/", "http://example.com/", "file:///etc/passwd",
                "ftp://payment-api/", "http://user:pw@payment-api/", "gopher://payment-api/", "not a url", "http:///nohost")) {
            assertThrows(ApiException.class, () -> strict.validate(bad), bad);
        }
    }

    @Test
    void blockedAddressRangesCoverInternalNetworks() throws Exception {
        for (String ip : List.of("127.0.0.1", "10.1.2.3", "172.16.0.1", "172.31.255.255", "192.168.1.1", "169.254.169.254", "100.64.0.1", "100.127.255.255", "0.0.0.0",
                "224.0.0.1", "255.255.255.255", "::1", "fe80::1", "fc00::1", "fd12:3456::1")) {
            assertTrue(UrlPolicy.isBlocked(InetAddress.getByName(ip)), ip + " must be blocked");
        }
        for (String ip : List.of("8.8.8.8", "1.1.1.1", "93.184.216.34", "100.63.255.255", "100.128.0.1", "172.32.0.1", "2606:4700:4700::1111")) {
            assertFalse(UrlPolicy.isBlocked(InetAddress.getByName(ip)), ip + " is public");
        }
    }

    // ---------------------------------------------------------------- event envelope / schema validation
    @Test
    void eventSchemaValidation() {
        String id = UUID.randomUUID().toString();
        String ok = "{\"eventId\":\"" + id + "\",\"eventType\":\"payment.capture.requested\",\"schemaVersion\":1,\"aggregateId\":\"" + id + "\",\"payload\":{\"paymentId\":\"" + id + "\"}}";
        assertEquals("payment.capture.requested", Events.parse(MAPPER, ok).eventType());
        assertEquals(UUID.fromString(id), Events.parse(MAPPER, ok).uuid("paymentId"));
        for (String bad : List.of("not json", "[]", ok.replace("\"schemaVersion\":1", "\"schemaVersion\":2"), ok.replace("\"schemaVersion\":1,", ""),
                ok.replace(id + "\",\"eventType", "nope\",\"eventType"), ok.replace("\"payload\":{\"paymentId\":\"" + id + "\"}", "\"payload\":5"))) {
            assertThrows(Events.InvalidEventException.class, () -> Events.parse(MAPPER, bad), bad);
        }
        assertThrows(Events.InvalidEventException.class, () -> Events.parse(MAPPER, ok).uuid("missing"));
    }
}
