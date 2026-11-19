package io.paycore;

import io.paycore.PaymentService.PaymentView;
import io.paycore.PaymentService.RefundView;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Money amounts are integer MINOR units (USD cents) in `amountMinor`; there are no decimals in the API.
 * Async operations (authorize, capture, refund) return 202 with the CURRENT state; poll GET /payments/{id} for the outcome.
 */
@RestController
@RequestMapping("/api/v1/payments")
@Tag(name = "Payments")
@ConditionalOnProperty(name = "paycore.roles.api", havingValue = "true")
public class PaymentController {
    public record CreatePayment(@NotNull Long amountMinor, @NotBlank String currency,
                                @NotBlank @Pattern(regexp = "^[A-Za-z0-9_-]{1,64}$", message = "synthetic customer id: 1-64 chars of [A-Za-z0-9_-]") String customerId) {}

    public record CaptureRequest(Long amountMinor) {}

    public record RefundRequest(@NotNull Long amountMinor) {}

    private final PaymentService payments;

    public PaymentController(PaymentService payments) {
        this.payments = payments;
    }

    static ResponseEntity<String> stored(Idempotency.Stored s) {
        var b = ResponseEntity.status(s.status()).contentType(MediaType.APPLICATION_JSON);
        if (s.replayed()) {
            b.header("Idempotent-Replayed", "true");
        }
        return b.body(s.body());
    }

    @PostMapping
    @Operation(summary = "Create a payment (201). Idempotency-Key required; same key + same body replays the original response, same key + different body is 409.")
    public ResponseEntity<String> create(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @RequestHeader("Idempotency-Key") String key, @Valid @RequestBody CreatePayment req) {
        return stored(payments.create(m.merchantId(), key, req));
    }

    @GetMapping("/{id}")
    public PaymentView get(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @PathVariable UUID id) {
        return payments.get(m.merchantId(), id).orElseThrow(() -> ApiException.notFound("payment"));
    }

    @GetMapping
    public Map<String, Object> list(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @RequestParam(required = false) String status,
                                    @RequestParam(required = false) String customerId, @RequestParam(required = false) Instant createdFrom,
                                    @RequestParam(required = false) Instant createdTo, @RequestParam(defaultValue = "0") int page,
                                    @RequestParam(defaultValue = "20") int size, @RequestParam(required = false) String sort) {
        return payments.list(m.merchantId(), status, customerId, createdFrom, createdTo, page, size, sort);
    }

    @PostMapping("/{id}/authorize")
    @Operation(summary = "Request authorization (202). The worker obtains the provider outcome asynchronously.")
    public ResponseEntity<Object> authorize(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @PathVariable UUID id) {
        var r = payments.authorize(m.merchantId(), id);
        return ResponseEntity.status(r.status()).body(r.body());
    }

    @PostMapping("/{id}/capture")
    @Operation(summary = "Request capture (202). Idempotency-Key required. At most one live capture per payment; the amount defaults to the authorized amount.")
    public ResponseEntity<String> capture(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @PathVariable UUID id,
                                          @RequestHeader("Idempotency-Key") String key, @RequestBody(required = false) CaptureRequest req) {
        return stored(payments.capture(m.merchantId(), id, key, req));
    }

    @PostMapping("/{id}/cancel")
    @Operation(summary = "Cancel an authorized payment that has no capture in flight or completed.")
    public PaymentView cancel(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @PathVariable UUID id) {
        return payments.cancel(m.merchantId(), id);
    }

    @PostMapping("/{id}/refunds")
    @Operation(summary = "Request a full or partial refund (202). Idempotency-Key required. 422 if it exceeds the remaining refundable amount, 409 if the payment is not captured.")
    public ResponseEntity<String> refund(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @PathVariable UUID id,
                                         @RequestHeader("Idempotency-Key") String key, @Valid @RequestBody RefundRequest req) {
        return stored(payments.refund(m.merchantId(), id, key, req));
    }

    @GetMapping("/{id}/refunds")
    public List<RefundView> refunds(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @PathVariable UUID id) {
        return payments.refunds(m.merchantId(), id);
    }
}
