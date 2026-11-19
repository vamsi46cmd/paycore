package io.paycore;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Topics are created by the applications themselves (idempotent), so a fresh checkout needs no manual initialization.
 * Ordering: all payment events/commands are keyed by paymentId, so they are ordered per payment within a partition; there is no
 * ordering across payments. Consumers do not depend on cross-event order: every handler is guarded by the payment's current state.
 * Error handling: transient errors retry with bounded exponential backoff, then the record goes to the dead-letter topic;
 * schema/validation errors skip retries and go straight to the dead-letter topic.
 */
@Configuration
public class KafkaConfig {
    @Bean
    KafkaAdmin.NewTopics paycoreTopics(Props p) {
        int parts = p.topics().partitions();
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(p.commandsTopic()).partitions(parts).replicas(1).build(),
                TopicBuilder.name(p.eventsTopic()).partitions(parts).replicas(1).build(),
                TopicBuilder.name(p.webhookEventsTopic()).partitions(parts).replicas(1).build(),
                TopicBuilder.name(p.reconciliationEventsTopic()).partitions(1).replicas(1).build(),
                TopicBuilder.name(p.dlqTopic()).partitions(1).replicas(1).build());
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template, Props p) {
        var recoverer = new DeadLetterPublishingRecoverer(template, (rec, ex) -> new TopicPartition(p.dlqTopic(), 0));
        var backoff = new ExponentialBackOffWithMaxRetries(p.worker().maxRetries());
        backoff.setInitialInterval(p.worker().backoffInitial().toMillis());
        backoff.setMultiplier(p.worker().backoffMultiplier());
        backoff.setMaxInterval(p.worker().backoffMax().toMillis());
        var handler = new DefaultErrorHandler(recoverer, backoff);
        handler.addNotRetryableExceptions(Events.InvalidEventException.class);
        return handler;
    }
}
