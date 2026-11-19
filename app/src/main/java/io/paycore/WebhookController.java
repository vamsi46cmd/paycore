package io.paycore;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/webhooks")
@Tag(name = "Webhooks")
@ConditionalOnProperty(name = "paycore.roles.api", havingValue = "true")
public class WebhookController {
    public record RegisterEndpoint(@NotBlank String url, @NotEmpty List<String> eventTypes) {}

    static final Set<String> EVENT_TYPES = Set.of("*", "payment.created", "payment.authorized", "payment.failed", "payment.captured",
            "payment.capture_failed", "payment.canceled", "refund.completed", "refund.failed");

    private final JdbcTemplate jdbc;
    private final UrlPolicy policy;
    private final SecureRandom random = new SecureRandom();

    public WebhookController(JdbcTemplate jdbc, UrlPolicy policy) {
        this.jdbc = jdbc;
        this.policy = policy;
    }

    /** The signing secret is returned ONCE, at registration. The destination must pass the SSRF policy (see UrlPolicy). */
    @PostMapping("/endpoints")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, Object> register(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @Valid @RequestBody RegisterEndpoint req) {
        policy.validate(req.url());
        for (String t : req.eventTypes()) {
            if (!EVENT_TYPES.contains(t)) {
                throw ApiException.badRequest("unknown event type: " + t);
            }
        }
        byte[] raw = new byte[24];
        random.nextBytes(raw);
        String secret = "whsec_" + HexFormat.of().formatHex(raw);
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO webhook_endpoints (id, merchant_id, url, secret, event_types) VALUES (?,?,?,?,?)", ps -> {
            ps.setObject(1, id);
            ps.setObject(2, m.merchantId());
            ps.setString(3, req.url());
            ps.setString(4, secret);
            ps.setArray(5, ps.getConnection().createArrayOf("text", req.eventTypes().toArray()));
        });
        return Map.of("endpointId", id, "url", req.url(), "eventTypes", req.eventTypes(), "secret", secret);
    }

    @GetMapping("/endpoints")
    public List<Map<String, Object>> endpoints(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m) {
        return Camel.list(jdbc.queryForList("SELECT id, url, event_types, active, created_at FROM webhook_endpoints WHERE merchant_id = ? ORDER BY created_at LIMIT 200", m.merchantId()));
    }

    @GetMapping("/deliveries")
    public List<Map<String, Object>> deliveries(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @RequestParam(required = false) String status,
                                                @RequestParam(defaultValue = "50") int limit) {
        if (limit < 1 || limit > 200) {
            throw ApiException.badRequest("limit must be between 1 and 200");
        }
        if (status != null && !Set.of("PENDING", "DELIVERED", "DEAD").contains(status)) {
            throw ApiException.badRequest("status must be PENDING, DELIVERED or DEAD");
        }
        return Camel.list(jdbc.queryForList("SELECT id, event_id, endpoint_id, event_type, status, attempt_count, next_attempt_at, last_error, created_at, delivered_at FROM webhook_deliveries "
                + "WHERE merchant_id = ?" + (status == null ? "" : " AND status = '" + status + "'") + " ORDER BY created_at DESC LIMIT ?", m.merchantId(), limit));
    }

    /** Re-queues a DEAD delivery (same event id, so receivers can still de-duplicate). */
    @PostMapping("/deliveries/{id}/replay")
    public Map<String, Object> replay(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @PathVariable UUID id) {
        int n = jdbc.update("UPDATE webhook_deliveries SET status = 'PENDING', attempt_count = 0, next_attempt_at = now(), last_error = NULL WHERE id = ? AND merchant_id = ? AND status = 'DEAD'", id, m.merchantId());
        if (n == 0) {
            Integer exists = jdbc.queryForObject("SELECT count(*) FROM webhook_deliveries WHERE id = ? AND merchant_id = ?", Integer.class, id, m.merchantId());
            if (exists == null || exists == 0) {
                throw ApiException.notFound("delivery");
            }
            throw ApiException.conflict("invalid_state", "Only DEAD deliveries can be replayed");
        }
        return Map.of("deliveryId", id, "status", "PENDING");
    }

    /**
     * LOCAL TEST RECEIVER (development only, paycore.webhooks.test-receiver-enabled). Verifies the HMAC signature with the endpoint's secret and
     * records every receipt. ?failFirst=N makes it answer 500 for the first N receipts of each event, to exercise retries.
     */
    @RestController
    @ConditionalOnProperty(name = {"paycore.roles.api", "paycore.webhooks.test-receiver-enabled"}, havingValue = "true")
    public static class TestReceiver {
        private final JdbcTemplate jdbc;

        public TestReceiver(JdbcTemplate jdbc) {
            this.jdbc = jdbc;
        }

        @PostMapping("/internal/test-receiver")
        public org.springframework.http.ResponseEntity<String> receive(@org.springframework.web.bind.annotation.RequestHeader(value = "PayCore-Endpoint-Id") UUID endpointId, @RequestParam(defaultValue = "0") int failFirst,
                                                                         @org.springframework.web.bind.annotation.RequestHeader(value = "PayCore-Signature", required = false) String signature,
                                                                         @org.springframework.web.bind.annotation.RequestHeader(value = "PayCore-Event-Id", required = false) String eventId,
                                                                         @RequestBody String body) {
            List<String> secrets = jdbc.queryForList("SELECT secret FROM webhook_endpoints WHERE id = ?", String.class, endpointId);
            boolean valid = !secrets.isEmpty() && Webhook.verify(secrets.get(0), signature, body, System.currentTimeMillis() / 1000, 300);
            UUID event = null;
            try {
                event = eventId == null ? null : UUID.fromString(eventId);
            } catch (IllegalArgumentException ignored) {
                // leave null
            }
            Integer prior = event == null ? 0 : jdbc.queryForObject("SELECT count(*) FROM test_webhook_receipts WHERE endpoint_id = ? AND event_id = ?", Integer.class, endpointId, event);
            int status = !valid ? 401 : (prior != null && prior < failFirst) ? 500 : 200;
            jdbc.update("INSERT INTO test_webhook_receipts (id, endpoint_id, event_id, signature_valid, responded_status) VALUES (?,?,?,?,?)", UUID.randomUUID(), endpointId, event, valid, status);
            return org.springframework.http.ResponseEntity.status(status).body(status == 200 ? "ok" : "rejected");
        }

        @GetMapping("/internal/test-receiver/{endpointId}/receipts")
        public Map<String, Object> receipts(@PathVariable UUID endpointId) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("total", jdbc.queryForObject("SELECT count(*) FROM test_webhook_receipts WHERE endpoint_id = ?", Integer.class, endpointId));
            out.put("validSignatures", jdbc.queryForObject("SELECT count(*) FROM test_webhook_receipts WHERE endpoint_id = ? AND signature_valid", Integer.class, endpointId));
            out.put("distinctEvents", jdbc.queryForObject("SELECT count(DISTINCT event_id) FROM test_webhook_receipts WHERE endpoint_id = ? AND responded_status = 200", Integer.class, endpointId));
            return out;
        }
    }
}
