# ADR 0005: Kafka consumer delivery guarantees

## Status
Accepted.

## Decision
Treat Kafka as at-least-once and make every consumer idempotent. Exactly-once is **not** claimed anywhere.
- Producer: `acks=all`, idempotent producer, bounded retries (idempotence only removes producer-side duplicates; it does not make the pipeline exactly-once).
- Consumer: manual offset semantics via `ack-mode=record`; an offset is committed after the handler returns. If the process dies mid-handler the record is redelivered.
- Idempotency of handlers, in three layers: (1) `processed_events` marker inserted in the same transaction as the effects; (2) conditional UPDATEs (`WHERE status = 'PROCESSING'`) so stale or replayed commands change nothing;
  (3) unique ledger references.
- Provider calls happen outside transactions with stable operation ids. After a timeout the worker looks the operation up first; it retries only when nothing was recorded, and reuses the same id.
- Errors: transient errors retry with bounded exponential backoff then go to `payments.dlq.v1`; schema/validation errors skip retries and go straight to the DLQ.
- Ordering: key = paymentId, so events of one payment are ordered within a partition; there is no cross-payment ordering and handlers do not depend on it.

## Known limits
- A message that exhausts its retries is dead-lettered and its payment stays PROCESSING/PENDING; reconciliation reports it as stuck and can safely re-queue it. There is no automatic DLQ replay.
- An unexpected failure after a provider success but before the database commit can leave a capture PENDING while the provider recorded success; reconciliation detects this (`PROVIDER_OUTCOME_NOT_APPLIED`) and its safe repair completes it.
