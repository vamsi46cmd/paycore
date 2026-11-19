package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class OutboxIT extends TestSupport {
    @Autowired OutboxPublisher publisher;

    private long headerCount(List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> recs, UUID eventId) {
        return recs.stream().filter(r -> {
            var h = r.headers().lastHeader("eventId");
            return h != null && eventId.toString().equals(new String(h.value()));
        }).count();
    }

    @Test
    void publisherCrashAfterBrokerAckCausesADuplicateButNeverALossOrADoubleEffect() throws Exception {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_outbox_crash", 1500);
        await("creation event drained", Duration.ofSeconds(20), () -> count("SELECT count(*) FROM outbox_events WHERE status = 'PENDING'"), n -> n == 0);

        publisher.armCrashAfterAck(1);   // the next batch dies after Kafka acks, before the DB transaction commits
        authorize(m, p);                  // creates exactly one outbox row: the authorize command
        UUID eventId = jdbc.queryForObject("SELECT id FROM outbox_events WHERE aggregate_id = ? AND event_type = 'payment.authorize.requested'", UUID.class, p);

        awaitPayment(m, p, "AUTHORIZED");   // no loss: the event is re-sent and processed
        await("outbox row finally published", Duration.ofSeconds(20), () -> jdbc.queryForObject("SELECT status FROM outbox_events WHERE id = ?", String.class, eventId), "PUBLISHED"::equals);

        long copies = headerCount(readTopic(commands()), eventId);
        assertTrue(copies >= 2, "the broker holds the event " + copies + " times: acked once before the crash, re-sent after (at-least-once)");
        assertEquals(1, count("SELECT count(*) FROM authorizations WHERE payment_id = ?", p), "...yet it had exactly one financial effect");
        assertEquals(1, count("SELECT count(*) FROM provider_operations WHERE operation_id = ?", "auth-" + p));
        assertEquals(1, count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'payment.authorized'", p));
    }

    @Test
    void severalPublishersShareTheBacklogWithoutPublishingARowTwice() throws Exception {
        UUID aggregate = UUID.randomUUID();
        Set<UUID> ids = new HashSet<>();
        for (int i = 0; i < 300; i++) {
            UUID id = UUID.randomUUID();
            ids.add(id);
            String envelope = "{\"eventId\":\"" + id + "\",\"eventType\":\"test.noop\",\"schemaVersion\":1,\"aggregateId\":\"" + aggregate + "\",\"payload\":{\"n\":" + i + "}}";
            jdbc.update("INSERT INTO outbox_events (id, aggregate_id, topic, event_type, payload) VALUES (?,?,?,?,?::jsonb)", id, aggregate, events(), "test.noop", envelope);
        }
        // four extra publishers race the scheduled one: SKIP LOCKED gives them disjoint rows
        runConcurrently(4, i -> () -> {
            int total = 0;
            for (int n = publisher.publishBatch(); n > 0; n = publisher.publishBatch()) {
                total += n;
            }
            return total;
        });
        await("backlog drained", Duration.ofSeconds(30), () -> count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND status = 'PENDING'", aggregate), n -> n == 0);
        Map<UUID, Integer> seen = new HashMap<>();
        for (var r : readTopic(events())) {
            var h = r.headers().lastHeader("eventId");
            if (h != null) {
                UUID id = UUID.fromString(new String(h.value()));
                if (ids.contains(id)) seen.merge(id, 1, Integer::sum);
            }
        }
        assertEquals(300, seen.size(), "no committed event was lost");
        assertEquals(0, seen.values().stream().filter(c -> c != 1).count(), "no event was published twice");
    }

    @Test
    void outboxRowIsCreatedAtomicallyWithTheStateChange() {
        Merchant m = newMerchant();
        UUID p = createPayment(m, "cust_atomic", 200);
        assertEquals(1, count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'payment.created'", p));
        // a business rejection (409) must leave no outbox row behind
        expect(409, send("POST", "/api/v1/payments/" + p + "/cancel", m.apiKey(), null));
        assertEquals(0, count("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'payment.canceled'", p));
    }
}
