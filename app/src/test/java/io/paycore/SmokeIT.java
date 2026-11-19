package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SmokeIT extends TestSupport {
    @Test
    void appStartsAndCreatesAPayment() {
        Merchant m = newMerchant();
        UUID id = createPayment(m, "cust_smoke", 1234);
        assertEquals("CREATED", payment(m, id).get("status").asText());
        assertTrue(count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?", id) >= 1);
    }
}
