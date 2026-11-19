# ADR 0002: Transactional outbox instead of publishing to Kafka directly

## Status
Accepted.

## Context
A state change must be followed by an event. Publishing to Kafka inside the request has the dual-write problem: if the process dies
between the database commit and the Kafka send, the event is lost (or, the other way around, an event exists for a change that rolled back).

## Decision
Write the event to an `outbox_events` table in the SAME transaction as the state change (`Outbox.enqueue` requires an active
transaction). A relay publishes pending rows to Kafka with `acks=all` and marks them PUBLISHED only after the broker acknowledged.
Several publishers cooperate with `SELECT ... FOR UPDATE SKIP LOCKED`; the relay runs in an explicit transaction so those locks last for the batch.

## Guarantees
- A committed event is never lost: it stays PENDING until acknowledged (verified by stopping Kafka mid-flow).
- Delivery is **at-least-once**: if the relay dies after the broker acks but before its transaction commits, the row is re-sent. Consumers are idempotent
  (processed-event markers, conditional state updates, unique ledger references). Verified by a crash-after-ack test.
- Events of one batch keep their order; there is no global ordering. Payment events share the key `paymentId` (ordered per payment per partition).

## Alternatives considered
- Direct publish: simpler, loses events on crashes.
- Kafka transactions / exactly-once semantics: only cover Kafka-to-Kafka; the database write is outside that boundary, so end-to-end exactly-once is not achieved.
- Change data capture (Debezium): robust, but adds an operational component this project does not need.

## Lessons recorded
The first relay used `@Transactional` and was invoked by `@Scheduled` on the same object, which bypasses Spring's proxy: the row locks were released after
each statement and two publishers sometimes took the same rows. It was found by a concurrency test and fixed with an explicit `TransactionTemplate`.
