package io.paycore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests run against REAL PostgreSQL, Kafka and Redis from docker compose (docker compose up -d postgres kafka redis),
 * using the separate paycore_test database and a unique Kafka topic prefix per run, and a real HTTP server on a random port.
 * One Spring context (all roles enabled) is shared by every IT class.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
abstract class TestSupport {
    static final String TOPIC_PREFIX = "it" + Long.toString(System.currentTimeMillis(), 36) + ".";
    static final String ADMIN_KEY = "integration-test-admin-key-0123456789";

    static {
        Map<String, String> p = Map.ofEntries(
            Map.entry("spring.datasource.url", System.getProperty("it.db.url", "jdbc:postgresql://localhost:5432/paycore_test")),
            Map.entry("spring.kafka.bootstrap-servers", System.getProperty("it.kafka", "localhost:29092")),
            Map.entry("management.server.port", "-1"),
            Map.entry("paycore.admin-key", ADMIN_KEY),
            Map.entry("paycore.topics.prefix", TOPIC_PREFIX),
            Map.entry("paycore.consumer-group-prefix", "it" + TOPIC_PREFIX.replace(".", "")),
            Map.entry("paycore.outbox.poll-interval", "PT0.1S"),
            Map.entry("paycore.worker.backoff-initial", "200ms"),
            Map.entry("paycore.worker.backoff-max", "1s"),
            Map.entry("paycore.worker.max-retries", "5"),
            Map.entry("paycore.provider.delay-ms", "100"),
            Map.entry("paycore.webhooks.backoff-base", "200ms"),
            Map.entry("paycore.webhooks.allowed-hosts", "localhost"),
            Map.entry("paycore.reconciliation.stuck-after", "3s"),
            Map.entry("paycore.reconciliation.schedule-enabled", "false"),
            Map.entry("paycore.ratelimit.per-second", "100000"));
        p.forEach(System::setProperty);
    }

    @LocalServerPort protected int port;
    @Autowired protected JdbcTemplate jdbc;
    @Autowired protected ObjectMapper om;
    @Autowired protected KafkaTemplate<String, String> kafkaTemplate;

    protected final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    record Resp(int status, JsonNode body, java.net.http.HttpHeaders headers) {}

    record Merchant(UUID id, String apiKey) {}

    protected Resp send(String method, String path, String apiKey, Object body, String... headers) {
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(20));
            if (apiKey != null) {
                b.header(apiKey.equals(ADMIN_KEY) ? "X-Admin-Key" : "X-Api-Key", apiKey);
            }
            for (int i = 0; i + 1 < headers.length; i += 2) {
                b.header(headers[i], headers[i + 1]);
            }
            HttpRequest.BodyPublisher pub = body == null ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body instanceof String s ? s : om.writeValueAsString(body));
            if (body != null) {
                b.header("Content-Type", "application/json");
            }
            HttpResponse<String> r = http.send(b.method(method, pub).build(), HttpResponse.BodyHandlers.ofString());
            JsonNode json;
            if (r.body() == null || r.body().isBlank()) {
                json = om.missingNode();
            } else {
                try {
                    json = om.readTree(r.body());
                } catch (Exception notJson) {
                    json = om.getNodeFactory().textNode(r.body());   // plain-text bodies (e.g. the test receiver's "ok")
                }
            }
            return new Resp(r.statusCode(), json, r.headers());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Resp expect(int status, Resp r) {
        assertEquals(status, r.status(), "unexpected response: " + r.body());
        return r;
    }

    protected Merchant newMerchant() {
        JsonNode b = expect(201, send("POST", "/api/v1/merchants", ADMIN_KEY, Map.of("name", "test-" + UUID.randomUUID()))).body();
        return new Merchant(UUID.fromString(b.get("merchantId").asText()), b.get("apiKey").asText());
    }

    protected UUID createPayment(Merchant m, String customer, long amountMinor) {
        JsonNode b = expect(201, send("POST", "/api/v1/payments", m.apiKey(), Map.of("amountMinor", amountMinor, "currency", "USD", "customerId", customer),
                "Idempotency-Key", UUID.randomUUID().toString())).body();
        return UUID.fromString(b.get("id").asText());
    }

    protected JsonNode payment(Merchant m, UUID id) {
        return expect(200, send("GET", "/api/v1/payments/" + id, m.apiKey(), null)).body();
    }

    protected void authorize(Merchant m, UUID id) {
        int s = send("POST", "/api/v1/payments/" + id + "/authorize", m.apiKey(), null).status();
        assertEquals(true, s == 202 || s == 200, "authorize status " + s);
    }

    protected UUID capture(Merchant m, UUID id, Long amount) {
        Resp r = expect(202, send("POST", "/api/v1/payments/" + id + "/capture", m.apiKey(), amount == null ? Map.of() : Map.of("amountMinor", amount), "Idempotency-Key", UUID.randomUUID().toString()));
        return UUID.fromString(r.body().get("captureId").asText());
    }

    protected Resp refund(Merchant m, UUID id, long amount, String key) {
        return send("POST", "/api/v1/payments/" + id + "/refunds", m.apiKey(), Map.of("amountMinor", amount), "Idempotency-Key", key);
    }

    /** Creates, authorizes and captures a payment and waits for CAPTURED. */
    protected UUID capturedPayment(Merchant m, String customer, long amount) throws InterruptedException {
        UUID id = createPayment(m, customer, amount);
        authorize(m, id);
        awaitPayment(m, id, "AUTHORIZED");
        capture(m, id, null);
        awaitPayment(m, id, "CAPTURED");
        return id;
    }

    protected JsonNode awaitPayment(Merchant m, UUID id, String status) throws InterruptedException {
        return await("payment " + id + " -> " + status, Duration.ofSeconds(40), () -> payment(m, id), p -> status.equals(p.get("status").asText()));
    }

    protected String awaitRefund(UUID refundId, String status) throws InterruptedException {
        return await("refund " + refundId + " -> " + status, Duration.ofSeconds(40),
                () -> jdbc.queryForObject("SELECT status FROM refunds WHERE id = ?", String.class, refundId), status::equals);
    }

    /** Bounded polling instead of sleeps: returns as soon as the condition holds, fails with the last value on timeout. */
    protected static <T> T await(String what, Duration timeout, Supplier<T> probe, Predicate<T> done) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        T last = null;
        while (System.nanoTime() < deadline) {
            last = probe.get();
            if (done.test(last)) {
                return last;
            }
            Thread.sleep(100);
        }
        fail("Timed out waiting for " + what + "; last value: " + last);
        return last;
    }

    protected long count(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0 : n;
    }

    /** Reads every record currently in a topic (a fresh consumer group each call), stopping after the topic stays quiet. */
    protected List<ConsumerRecord<String, String>> readTopic(String topic) {
        Properties pr = new Properties();
        pr.put("bootstrap.servers", System.getProperty("it.kafka", "localhost:29092"));
        pr.put("group.id", "reader-" + UUID.randomUUID());
        pr.put("auto.offset.reset", "earliest");
        pr.put("enable.auto.commit", "false");
        pr.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pr.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(pr)) {
            c.subscribe(List.of(topic));
            int quiet = 0;
            while (quiet < 3) {
                var recs = c.poll(Duration.ofSeconds(1));
                if (recs.isEmpty()) {
                    quiet++;
                } else {
                    quiet = 0;
                    recs.forEach(out::add);
                }
            }
        }
        return out;
    }

    protected static String events() {
        return TOPIC_PREFIX + "payments.events.v1";
    }

    protected static String commands() {
        return TOPIC_PREFIX + "payments.commands.v1";
    }

    protected static String dlq() {
        return TOPIC_PREFIX + "payments.dlq.v1";
    }

    /** Publishes a raw message (used to inject duplicates and invalid messages). */
    protected void sendRaw(String topic, String key, String value) throws Exception {
        kafkaTemplate.send(topic, key, value).get(10, java.util.concurrent.TimeUnit.SECONDS);
    }

    /** Waits until a consumer group has committed everything currently in a topic (lag 0): the deterministic replacement for sleeping. */
    protected void awaitCaughtUp(String groupSuffix, String topic) throws InterruptedException {
        String group = System.getProperty("paycore.consumer-group-prefix") + "-" + groupSuffix;
        try (Admin admin = Admin.create(Map.<String, Object>of("bootstrap.servers", System.getProperty("it.kafka", "localhost:29092")))) {
            await("group " + group + " to catch up on " + topic, Duration.ofSeconds(40), () -> {
                try {
                    Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
                    var parts = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions();
                    Map<TopicPartition, OffsetSpec> req = new java.util.HashMap<>();
                    parts.forEach(pi -> req.put(new TopicPartition(topic, pi.partition()), OffsetSpec.latest()));
                    var ends = admin.listOffsets(req).all().get();
                    long lag = 0;
                    for (var e : ends.entrySet()) {
                        OffsetAndMetadata c = committed.get(e.getKey());
                        lag += e.getValue().offset() - (c == null ? 0 : c.offset());
                    }
                    return lag;
                } catch (Exception ex) {
                    return Long.MAX_VALUE;
                }
            }, l -> l == 0);
        }
    }

    /** Releases {@code n} callers at the same instant so they genuinely race. */
    <T> List<T> runConcurrently(int n, java.util.function.IntFunction<Callable<T>> task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch ready = new CountDownLatch(n);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Callable<T> c = task.apply(i);
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                return c.call();
            }));
        }
        ready.await();
        go.countDown();
        List<T> out = new ArrayList<>();
        for (Future<T> f : futures) {
            out.add(f.get());
        }
        pool.shutdown();
        return out;
    }
}
