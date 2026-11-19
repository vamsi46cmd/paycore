package io.paycore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Outbox relay. Each batch runs in one transaction: rows are locked with FOR UPDATE SKIP LOCKED (so several publishers,
 * in any process, take disjoint rows), sent to Kafka, and only rows whose broker ack arrived are marked PUBLISHED.
 *
 * Guarantee: a committed event is never lost (it stays PENDING until acknowledged) but it MAY be delivered more than once:
 * if the process dies after Kafka acks and before this transaction commits, the row is re-sent. Consumers are idempotent.
 * Batches stop at the first failure so events of one batch keep their order.
 */
@Component
@ConditionalOnProperty(name = "paycore.outbox.enabled", havingValue = "true")
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    /** Thrown by the test hook to simulate a crash after the broker acknowledged but before the DB commit. */
    public static class SimulatedCrash extends RuntimeException {
        public SimulatedCrash() {
            super("simulated publisher crash after broker ack");
        }
    }

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final Props props;
    private final ObjectMapper mapper;
    private final Counter published;
    private final Counter failures;
    private final AtomicInteger crashAfterAck;
    private final AtomicLong backlog = new AtomicLong();
    private final TransactionTemplate tx;

    public OutboxPublisher(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, Props props, ObjectMapper mapper, MeterRegistry registry, PlatformTransactionManager tm) {
        this.tx = new TransactionTemplate(tm);
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.props = props;
        this.mapper = mapper;
        this.published = registry.counter("paycore.outbox.published");
        this.failures = registry.counter("paycore.outbox.publish.failures");
        this.crashAfterAck = new AtomicInteger(props.testHooks().outboxCrashAfterAck());
        Gauge.builder("paycore.outbox.backlog", backlog, AtomicLong::get).description("Outbox rows not yet published").register(registry);
    }

    /** Test hook: the next {@code n} batches crash after the first acknowledged send. */
    public void armCrashAfterAck(int n) {
        crashAfterAck.set(n);
    }

    @Scheduled(fixedDelayString = "${paycore.outbox.poll-interval}")
    public void tick() {
        try {
            publishBatch();
        } catch (SimulatedCrash e) {
            log.warn("{}", e.getMessage());
        } catch (Exception e) {
            failures.increment();
            log.warn("Outbox batch failed (events stay PENDING and will be retried): {}", e.toString());
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void refreshBacklog() {
        try {
            Long n = jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE status = 'PENDING'", Long.class);
            backlog.set(n == null ? 0 : n);
        } catch (Exception ignored) {
            // metric only
        }
    }

    private record Row(UUID id, UUID aggregateId, String topic, String payload) {}

    /**
     * Runs one batch in an EXPLICIT transaction. (A @Transactional annotation would be silently skipped when the scheduler calls
     * this method on the same object, which releases the row locks after every statement and lets two publishers take the same rows.)
     */
    public int publishBatch() {
        Integer n = tx.execute(s -> publishInTransaction());
        return n == null ? 0 : n;
    }

    private int publishInTransaction() {
        List<Row> rows = jdbc.query("SELECT id, aggregate_id, topic, payload::text AS payload FROM outbox_events WHERE status = 'PENDING' "
                + "ORDER BY created_at, id LIMIT ? FOR UPDATE SKIP LOCKED",
                (rs, i) -> new Row(rs.getObject("id", UUID.class), rs.getObject("aggregate_id", UUID.class), rs.getString("topic"), rs.getString("payload")),
                props.outbox().batchSize());
        List<UUID> acked = new ArrayList<>();
        for (Row r : rows) {
            try {
                ProducerRecord<String, String> rec = new ProducerRecord<>(r.topic(), r.aggregateId().toString(), r.payload());
                rec.headers().add("eventId", r.id().toString().getBytes(StandardCharsets.UTF_8));
                JsonNode corr = mapper.readTree(r.payload()).get("correlationId");
                if (corr != null) {
                    rec.headers().add("X-Correlation-Id", corr.asText().getBytes(StandardCharsets.UTF_8));
                }
                kafka.send(rec).get(5, TimeUnit.SECONDS);
                acked.add(r.id());
                if (crashAfterAck.get() > 0 && crashAfterAck.getAndDecrement() > 0) {
                    log.warn("simulating crash after ack: batch of {} rows, {} already acknowledged by Kafka", rows.size(), acked.size());
                    throw new SimulatedCrash();   // rolls the transaction back: acked rows stay PENDING -> duplicates later
                }
            } catch (SimulatedCrash e) {
                throw e;
            } catch (Exception e) {
                failures.increment();
                log.warn("outbox row {} not acknowledged by Kafka, stopping batch: {}", r.id(), e.toString());
                jdbc.update("UPDATE outbox_events SET attempts = attempts + 1, last_error = ? WHERE id = ?", abbreviate(e.toString()), r.id());
                break;
            }
        }
        if (!acked.isEmpty()) {
            jdbc.update("UPDATE outbox_events SET status = 'PUBLISHED', published_at = now(), attempts = attempts + 1 WHERE id = ANY (?)",
                    ps -> ps.setArray(1, ps.getConnection().createArrayOf("uuid", acked.toArray())));
            published.increment(acked.size());
        }
        return acked.size();
    }

    private static String abbreviate(String s) {
        return s.length() <= 300 ? s : s.substring(0, 300);
    }
}
