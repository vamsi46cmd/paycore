package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;

/** The per-merchant limit itself (a separate Spring context with a limit of 5 requests per second). */
@TestPropertySource(properties = "paycore.ratelimit.per-second=5")
class RateLimitIT extends TestSupport {
    @Test
    void exceedingTheLimitReturns429WithRetryAfterAndOtherMerchantsAreUnaffected() throws Exception {
        Merchant a = newMerchant();
        Merchant b = newMerchant();
        int ok = 0;
        int limited = 0;
        Resp lastLimited = null;
        for (int i = 0; i < 40; i++) {
            Resp r = send("GET", "/api/v1/payments", a.apiKey(), null);
            if (r.status() == 200) ok++;
            else if (r.status() == 429) { limited++; lastLimited = r; }
            else throw new AssertionError("unexpected status " + r.status() + " " + r.body());
        }
        assertTrue(limited > 0, "40 rapid requests against a limit of 5/s must hit the limit (ok=" + ok + ", limited=" + limited + ")");
        assertTrue(ok >= 5 && ok <= 10, "a fixed one-second window lets 5 through per window (at most two windows touched): ok=" + ok);
        assertEquals("rate_limited", lastLimited.body().get("code").asText());
        assertEquals("1", lastLimited.headers().firstValue("Retry-After").orElse(null));
        // the limit is per merchant: merchant B is not affected by A's traffic
        assertEquals(200, send("GET", "/api/v1/payments", b.apiKey(), null).status());
        // and it recovers by itself in the next window
        await("merchant A served again", Duration.ofSeconds(10), () -> send("GET", "/api/v1/payments", a.apiKey(), null).status(), s -> s == 200);
    }
}
