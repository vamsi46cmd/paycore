package io.paycore;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("paycore")
public record Props(String adminKey, Roles roles, Topics topics, String consumerGroupPrefix, Idempotency idempotency,
                    Outbox outbox, Worker worker, Provider provider, Webhooks webhooks, Reconciliation reconciliation,
                    RateLimit ratelimit, TestHooks testHooks) {
    public record Roles(boolean api, boolean worker, boolean webhook) {}

    public record Topics(String prefix, int partitions) {}

    public record Idempotency(Duration ttl) {}

    public record Outbox(boolean enabled, Duration pollInterval, int batchSize) {}

    public record Worker(int concurrency, int maxRetries, Duration backoffInitial, double backoffMultiplier, Duration backoffMax) {}

    public record Provider(long delayMs, int unavailableFailures) {}

    public record Webhooks(List<String> allowedHosts, int maxAttempts, Duration backoffBase, int batchSize, Duration timeout,
                           boolean testReceiverEnabled, boolean allowPublicHosts) {}

    public record Reconciliation(Duration stuckAfter, boolean scheduleEnabled, Duration interval) {}

    public record RateLimit(int perSecond) {}

    public record TestHooks(int outboxCrashAfterAck) {}

    public String commandsTopic() { return topics.prefix() + "payments.commands.v1"; }

    public String eventsTopic() { return topics.prefix() + "payments.events.v1"; }

    public String dlqTopic() { return topics.prefix() + "payments.dlq.v1"; }

    public String webhookEventsTopic() { return topics.prefix() + "webhooks.events.v1"; }

    public String reconciliationEventsTopic() { return topics.prefix() + "reconciliation.events.v1"; }

    public String group(String name) { return consumerGroupPrefix + "-" + name; }
}
