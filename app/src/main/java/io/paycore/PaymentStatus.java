package io.paycore;

import java.util.EnumSet;
import java.util.Set;

/**
 * CREATED -> PROCESSING            (authorize requested)
 * PROCESSING -> AUTHORIZED | FAILED (provider outcome)
 * AUTHORIZED -> CAPTURED | CANCELED
 * CAPTURED, FAILED, CANCELED are terminal. The same rules are enforced by a PostgreSQL trigger on payments.
 */
public enum PaymentStatus {
    CREATED, PROCESSING, AUTHORIZED, CAPTURED, FAILED, CANCELED;

    public Set<PaymentStatus> allowedNext() {
        return switch (this) {
            case CREATED -> EnumSet.of(PROCESSING);
            case PROCESSING -> EnumSet.of(AUTHORIZED, FAILED);
            case AUTHORIZED -> EnumSet.of(CAPTURED, CANCELED);
            case CAPTURED, FAILED, CANCELED -> EnumSet.noneOf(PaymentStatus.class);
        };
    }

    public boolean canTransitionTo(PaymentStatus next) {
        return allowedNext().contains(next);
    }

    public boolean isTerminal() {
        return allowedNext().isEmpty();
    }
}
