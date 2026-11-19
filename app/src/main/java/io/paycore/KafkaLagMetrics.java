package io.paycore;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Consumer lag per group (log end offset minus committed offset, summed). NaN means "not measurable right now", never a fake zero. */
@Component
public class KafkaLagMetrics {
    private final Props props;
    private final KafkaAdmin kafkaAdmin;
    private final Map<String, AtomicReference<Double>> lag = Map.of("payment-worker", new AtomicReference<>(Double.NaN), "webhook-worker", new AtomicReference<>(Double.NaN));
    private Admin admin;

    public KafkaLagMetrics(Props props, KafkaAdmin kafkaAdmin, MeterRegistry registry) {
        this.props = props;
        this.kafkaAdmin = kafkaAdmin;
        lag.forEach((name, ref) -> Gauge.builder("paycore.kafka.consumer.lag", ref, AtomicReference::get).tag("group", name).register(registry));
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 10000)
    public void refresh() {
        for (String name : lag.keySet()) {
            lag.get(name).set(measure(props.group(name)));
        }
    }

    private synchronized double measure(String group) {
        try {
            if (admin == null) {
                admin = Admin.create(kafkaAdmin.getConfigurationProperties());
            }
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(3, TimeUnit.SECONDS);
            if (committed.isEmpty()) {
                return Double.NaN;
            }
            var ends = admin.listOffsets(committed.keySet().stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()))).all().get(3, TimeUnit.SECONDS);
            double total = 0;
            for (var e : committed.entrySet()) {
                total += Math.max(0, ends.get(e.getKey()).offset() - e.getValue().offset());
            }
            return total;
        } catch (Exception e) {
            return Double.NaN;
        }
    }
}
